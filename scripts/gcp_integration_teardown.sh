#!/usr/bin/env bash
#
# Remove what the integration setup created.
#
#   ./scripts/gcp_integration_teardown.sh YOUR_PROJECT_ID [REGION]
#
# NOT removed, because GCP does not allow it: Cloud KMS key rings and keys are
# permanent. This script destroys the key's active *versions*, which stops the
# ~$0.06/version/month charge, but the ring and key resources remain forever.
#
set -euo pipefail

PROJECT="${1:?usage: $0 PROJECT_ID [REGION]}"
REGION="${2:-us-central1}"
PREFIX="cloudrift-it"
BUCKET="${PREFIX}-${PROJECT}"

say() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
gcloud config set project "${PROJECT}" >/dev/null

say "Deleting bucket gs://${BUCKET} and its contents"
gcloud storage rm --recursive "gs://${BUCKET}" 2>/dev/null || echo "not present"

say "Deleting any leftover ${PREFIX} Pub/Sub subscriptions and topics"
# The suite cleans these up itself; this catches litter from an interrupted run.
for sub in $(gcloud pubsub subscriptions list --format='value(name)' 2>/dev/null | grep "${PREFIX}" || true); do
  gcloud pubsub subscriptions delete "${sub}" --quiet && echo "deleted ${sub}"
done
for topic in $(gcloud pubsub topics list --format='value(name)' 2>/dev/null | grep "${PREFIX}" || true); do
  gcloud pubsub topics delete "${topic}" --quiet && echo "deleted ${topic}"
done

say "Deleting any leftover ${PREFIX} secrets"
for secret in $(gcloud secrets list --format='value(name)' 2>/dev/null | grep "${PREFIX}" || true); do
  gcloud secrets delete "${secret}" --quiet && echo "deleted ${secret}"
done

say "Destroying KMS key versions (ring and key itself cannot be deleted)"
KEYRING="${PREFIX}-ring"
KEYNAME="${PREFIX}-key"
for version in $(gcloud kms keys versions list --key "${KEYNAME}" --keyring "${KEYRING}" \
  --location "${REGION}" --filter='state:ENABLED' --format='value(name)' 2>/dev/null || true); do
  gcloud kms keys versions destroy "$(basename "${version}")" \
    --key "${KEYNAME}" --keyring "${KEYRING}" --location "${REGION}" --quiet \
    && echo "scheduled destruction of version $(basename "${version}")"
done

say "Deleting Firestore database ${PREFIX}-mongo"
gcloud firestore databases delete --database "${PREFIX}-mongo" --quiet 2>/dev/null \
  || echo "not present, or needs --etag / delete protection disabled"

say "Teardown complete"
