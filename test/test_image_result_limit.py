from __future__ import annotations

from unittest import mock

import pytest

from services.protocol import openai_v1_image_edit, openai_v1_image_generations
from services.protocol.conversation import ImageOutput, collect_image_outputs, stream_image_chunks


def extra_outputs(completed):
    try:
        for index in range(1, 4):
            yield ImageOutput("result", "gpt-image-2", index, 3,
                              data=[{"url": f"https://test/{index}-a"}, {"url": f"https://test/{index}-b"}])
    finally:
        completed.append(True)


@pytest.mark.parametrize("module", [openai_v1_image_generations, openai_v1_image_edit])
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("n", [1, 2])
def test_image_endpoints_return_at_most_requested_n_and_drain_outputs(module, stream, n):
    completed = []
    with mock.patch.object(module, "stream_image_outputs_with_pool", return_value=extra_outputs(completed)):
        result = module.handle({"prompt": "cat", "n": n, "stream": stream, "images": [(b"image", "test.png", "image/png")]})
        data = [item for chunk in result for item in chunk["data"]] if stream else result["data"]
    assert len(data) == n
    assert completed == [True]


def test_generic_collect_preserves_multi_image_results():
    assert len(collect_image_outputs(extra_outputs([]))["data"]) == 6
    assert sum(len(chunk["data"]) for chunk in stream_image_chunks(extra_outputs([]))) == 6
