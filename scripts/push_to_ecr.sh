#!/usr/bin/env bash
# Builds the pipeline image (linux/amd64) and pushes it to Amazon ECR.
# Creates the ECR repository the first time.
#   ./scripts/push_to_ecr.sh [region] [repository] [tag]
set -euo pipefail

REGION="${1:-us-east-1}"
REPOSITORY="${2:-nyc-taxi-duckdb-dbt}"
TAG="${3:-latest}"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE="${REGISTRY}/${REPOSITORY}:${TAG}"

if ! aws ecr describe-repositories --region "$REGION" --repository-names "$REPOSITORY" >/dev/null 2>&1; then
    echo "Creating ECR repository $REPOSITORY in $REGION"
    aws ecr create-repository --region "$REGION" --repository-name "$REPOSITORY" \
        --image-scanning-configuration scanOnPush=true >/dev/null
fi

aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"
docker build --platform linux/amd64 -t "$IMAGE" "$PROJECT_ROOT"
docker push "$IMAGE"

echo "Pushed $IMAGE"
