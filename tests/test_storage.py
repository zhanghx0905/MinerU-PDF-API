import io

import pytest

from app.storage import LocalStorage, S3Storage


def test_local_storage_rejects_paths_and_preserves_data(tmp_path):
    store = LocalStorage(tmp_path)
    key = "inputs/" + "a" * 32 + ".pdf"
    store.put(key, b"pdf bytes")
    assert store.get(key) == b"pdf bytes"
    with pytest.raises(ValueError):
        store.get("../private.pdf")
    store.delete(key)
    store.delete(key)
    assert not list(tmp_path.rglob("*.pdf"))


def test_s3_storage_uses_opaque_keys_and_closes_response(monkeypatch):
    pytest.importorskip("boto3")
    from botocore.response import StreamingBody
    from botocore.stub import Stubber

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    store = S3Storage("sample-bucket")
    key = "results/" + "b" * 32 + ".md"
    stream = io.BytesIO(b"markdown")
    with Stubber(store.client) as stub:
        stub.add_response(
            "put_object", {}, {"Bucket": "sample-bucket", "Key": key, "Body": b"markdown"}
        )
        stub.add_response(
            "get_object",
            {"Body": StreamingBody(stream, 8)},
            {"Bucket": "sample-bucket", "Key": key},
        )
        stub.add_response("delete_object", {}, {"Bucket": "sample-bucket", "Key": key})
        store.put(key, b"markdown")
        assert store.get(key) == b"markdown" and stream.closed
        store.delete(key)
        stub.assert_no_pending_responses()
