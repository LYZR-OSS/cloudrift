#!/usr/bin/env bash
#
# Provision the GCP resources the live integration suite needs, then print the
# environment variables to export.
#
#   ./scripts/gcp_integration_setup.sh YOUR_PROJECT_ID [REGION]
#
# Everything created is named with the `cloudrift-it` prefix.
# `gcp_integration_teardown.sh` removes what can be removed — read the WARNING
# about Cloud KMS below before running this.
#
set -euo pipefail

PROJECT="${1:?usage: $0 PROJECT_ID [REGION]}"
REGION="${2:-us-central1}"
PREFIX="cloudrift-it"
BUCKET="${PREFIX}-${PROJECT}"
KEYRING="${PREFIX}-ring"
KEYNAME="${PREFIX}-key"
FIRESTORE_DB="${PREFIX}-mongo"
# Firestore MongoDB compatibility needs a regional location, not multi-region.
FIRESTORE_LOCATION="${REGION}"

say() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }

say "Project: ${PROJECT}   Region: ${REGION}"
gcloud config set project "${PROJECT}" >/dev/null

say "Enabling APIs (idempotent, may take a minute)"
gcloud services enable \
  storage.googleapis.com \
  pubsub.googleapis.com \
  secretmanager.googleapis.com \
  cloudkms.googleapis.com \
  firestore.googleapis.com \
  iamcredentials.googleapis.com \
  --project "${PROJECT}"

say "Creating GCS bucket gs://${BUCKET}"
if gcloud storage buckets describe "gs://${BUCKET}" >/dev/null 2>&1; then
  echo "already exists"
else
  gcloud storage buckets create "gs://${BUCKET}" \
    --project "${PROJECT}" --location "${REGION}" --uniform-bucket-level-access
fi

# WARNING: KMS key rings and keys can NEVER be deleted — only key *versions* can
# be destroyed. This leaves a permanent (but tiny, ~$0.06/month per active
# version) artifact in the project. Nothing else here is permanent.
say "Creating KMS key ring + key (PERMANENT — see comment in this script)"
if gcloud kms keyrings describe "${KEYRING}" --location "${REGION}" >/dev/null 2>&1; then
  echo "key ring already exists"
else
  gcloud kms keyrings create "${KEYRING}" --location "${REGION}"
fi
if gcloud kms keys describe "${KEYNAME}" --keyring "${KEYRING}" --location "${REGION}" >/dev/null 2>&1; then
  echo "key already exists"
else
  gcloud kms keys create "${KEYNAME}" \
    --keyring "${KEYRING}" --location "${REGION}" --purpose encryption
fi
KMS_KEY="projects/${PROJECT}/locations/${REGION}/keyRings/${KEYRING}/cryptoKeys/${KEYNAME}"

say "Creating Firestore database '${FIRESTORE_DB}' in MongoDB-compatibility mode"
if gcloud firestore databases describe --database "${FIRESTORE_DB}" >/dev/null 2>&1; then
  echo "already exists"
else
  # --edition=enterprise + MONGODB_COMPATIBLE_API is what yields a wire-protocol
  # endpoint; a default/native database would NOT work with this library.
  gcloud firestore databases create \
    --database "${FIRESTORE_DB}" \
    --location "${FIRESTORE_LOCATION}" \
    --edition enterprise \
    --database-edition enterprise \
    --type firestore-native 2>/dev/null \
  || gcloud alpha firestore databases create \
    --database "${FIRESTORE_DB}" \
    --location "${FIRESTORE_LOCATION}" \
    --edition enterprise
fi

say "Resolving Firestore connection details"
# The UID in the endpoint hostname is NOT the database ID — read it back.
CONN_STRING="$(gcloud firestore databases connection-string \
  --database "${FIRESTORE_DB}" --format='value(connection_string)' 2>/dev/null || true)"
if [[ -n "${CONN_STRING}" ]]; then
  FIRESTORE_UID="$(sed -E 's#^mongodb://([^.]+)\..*#\1#' <<<"${CONN_STRING}")"
else
  echo "WARNING: could not read the connection string automatically." >&2
  echo "Run: gcloud firestore databases connection-string --database ${FIRESTORE_DB}" >&2
  FIRESTORE_UID="REPLACE_ME"
fi

say "Granting the current principal the needed roles"
PRINCIPAL="$(gcloud config get-value account)"
for role in \
  roles/storage.objectAdmin \
  roles/pubsub.admin \
  roles/secretmanager.admin \
  roles/cloudkms.cryptoKeyEncrypterDecrypter \
  roles/datastore.user
do
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member "user:${PRINCIPAL}" --role "${role}" \
    --condition=None >/dev/null 2>&1 \
    && echo "granted ${role}" \
    || echo "could not grant ${role} (may already hold it, or lack permission)"
done

cat <<EOF

$(say "Done — export these, then run the suite")

export CLOUDRIFT_GCP_PROJECT="${PROJECT}"
export CLOUDRIFT_GCS_BUCKET="${BUCKET}"
export CLOUDRIFT_GCP_KMS_KEY="${KMS_KEY}"
export CLOUDRIFT_FIRESTORE_UID="${FIRESTORE_UID}"
export CLOUDRIFT_FIRESTORE_LOCATION="${FIRESTORE_LOCATION}"
export CLOUDRIFT_FIRESTORE_DATABASE="${FIRESTORE_DB}"

# Signed-URL tests: only needed when your credential has no local private key
# (i.e. gcloud ADC or Workload Identity). Point it at a service account you can
# impersonate, and grant yourself roles/iam.serviceAccountTokenCreator on it.
# export CLOUDRIFT_GCS_SIGNER_SA="svc@${PROJECT}.iam.gserviceaccount.com"

# Memorystore is VPC-private and unreachable from a laptop — set these only when
# running from a VM/pod inside the VPC.
# export CLOUDRIFT_MEMORYSTORE_HOST="10.x.x.x"
# export CLOUDRIFT_MEMORYSTORE_AUTH="..."

Then:
  gcloud auth application-default login    # if you have not already
  uv run pytest tests/integration -v

EOF
