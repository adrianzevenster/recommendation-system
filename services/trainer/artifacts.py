"""MinIO / S3 persistence of model artifacts (FAISS index, vectorizer, scaler, LR)."""
import io
import json

import boto3
import joblib
from botocore.config import Config as BotocoreConfig

from common.ann import serialize_index
from common.config import settings
from common.metrics import ARTIFACTS_SAVED

from .runtime import _utcnow, logger


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=BotocoreConfig(signature_version="s3v4"),
    )


def _save_ann_artifacts(version: str, ann_index, id_list: list[str]) -> None:
    """Persist FAISS index + ID mapping to MinIO alongside other model artifacts."""
    if ann_index is None or not id_list:
        return
    try:
        client = _get_s3_client()
        # FAISS binary
        idx_bytes = serialize_index(ann_index)
        client.put_object(
            Bucket="models",
            Key=f"trainer/{version}/ann_index.faiss",
            Body=idx_bytes,
            ContentLength=len(idx_bytes),
        )
        # ID list (JSON array — order must match FAISS row order)
        id_bytes = json.dumps(id_list).encode()
        client.put_object(
            Bucket="models",
            Key=f"trainer/{version}/ann_index_ids.json",
            Body=id_bytes,
            ContentLength=len(id_bytes),
        )
        logger.info("Saved ANN index to MinIO", extra={"version": version, "items": len(id_list)})
    except Exception as exc:
        logger.warning("Failed to save ANN index", extra={"error": str(exc)})


def _save_model_artifacts(version: str, vectorizer, scaler, lr_model) -> None:
    try:
        client = _get_s3_client()
        try:
            client.create_bucket(Bucket="models")
        except Exception:
            pass

        artifacts = {"vectorizer": vectorizer, "scaler": scaler, "lr_model": lr_model}
        for name, artifact in artifacts.items():
            if artifact is None:
                continue
            buf = io.BytesIO()
            joblib.dump(artifact, buf)
            size = buf.tell()
            buf.seek(0)
            key = f"trainer/{version}/{name}.joblib"
            client.put_object(Bucket="models", Key=key, Body=buf, ContentLength=size)

        # Write a stable latest.json manifest so loaders never have to sort version strings
        manifest = json.dumps({"version": version, "saved_at": _utcnow().isoformat()}).encode()
        client.put_object(
            Bucket="models", Key="latest.json",
            Body=manifest, ContentLength=len(manifest),
        )

        ARTIFACTS_SAVED.inc()
        logger.info("Saved model artifacts to MinIO", extra={"version": version})
    except Exception as exc:
        logger.warning("Failed to save model artifacts", extra={"error": str(exc)})


def _load_latest_model_artifacts() -> tuple | None:
    try:
        client = _get_s3_client()

        # Prefer the manifest over lexicographic sort — it's the authoritative pointer
        try:
            resp = client.get_object(Bucket="models", Key="latest.json")
            manifest = json.loads(resp["Body"].read())
            latest = manifest["version"]
        except Exception:
            # Fall back to lexicographic sort for backward compatibility
            response = client.list_objects_v2(Bucket="models", Prefix="trainer/")
            objects = response.get("Contents", [])
            if not objects:
                return None
            versions: set[str] = set()
            for obj in objects:
                parts = obj["Key"].split("/")
                if len(parts) >= 3:
                    versions.add(parts[1])
            if not versions:
                return None
            latest = sorted(versions)[-1]

        result = {}
        for name in ("vectorizer", "scaler", "lr_model"):
            key = f"trainer/{latest}/{name}.joblib"
            try:
                resp = client.get_object(Bucket="models", Key=key)
                buf = io.BytesIO(resp["Body"].read())
                result[name] = joblib.load(buf)
            except Exception:
                result[name] = None

        return result.get("vectorizer"), result.get("scaler"), result.get("lr_model")
    except Exception as exc:
        logger.warning("Failed to load model artifacts", extra={"error": str(exc)})
        return None
