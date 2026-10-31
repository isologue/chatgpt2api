"""No-image outcomes lower score without changing authentication health or policy rules."""
from unittest.mock import Mock

import pytest
from services.protocol import conversation as flow
from test.test_image_download_account_switch import scenario


@pytest.mark.parametrize("policy", [False, True])
@pytest.mark.parametrize("as_error", [False, True])
def test_no_image_deducts_score_but_policy_never_does(scenario, monkeypatch, policy, as_error):
    backend = Mock()
    monkeypatch.setattr(flow, "OpenAIBackendAPI", lambda **kwargs: backend)
    text = "content_policy_violation: blocked by safety policy" if policy else "I cannot generate an image right now."
    # Progress is emitted first, matching the actual upstream SSE flow.
    monkeypatch.setattr(flow, "stream_image_outputs", lambda *args: iter([
        flow.ImageOutput(kind="progress", model="gpt-image-2", index=1, total=1, text="working"),
        flow.ImageOutput(kind="message", model="gpt-image-2", index=1, total=1, text=text),
    ]))
    request = flow.ConversationRequest(model="gpt-image-2", prompt="sandbox", message_as_error=as_error)
    if as_error:
        with pytest.raises(flow.ImageGenerationError) as error:
            flow._generate_single_image(request, 1, 1)
        assert error.value.code == ("content_policy_violation" if policy else "no_image_generated")
    else:
        flow._generate_single_image(request, 1, 1)
    used = [item for item in scenario.service._accounts.values() if item.get("last_used_at")]
    assert len(used) == 1
    account = used[0]
    assert account["image_score"] == (50 if policy else 45)
    assert account["fail"] == (0 if policy else 1)
    assert account["status"] == "正常" and account["quota"] == 10
    assert bool(account["last_policy_confirmed_at"]) == policy
    assert not scenario.service._image_inflight
    assert backend.close.call_count == 1


def test_no_image_penalty_floors_at_zero_and_preserves_success(scenario):
    service = scenario.service
    service.update_account("0", {"image_score": 3})
    service.mark_image_started("0")
    result = service.mark_image_result("0", False, outcome="no_image_generated")
    assert result["image_score"] == 0 and result["fail"] == 1
    assert result["image_score_updated_at"] > 0
    assert not service._image_inflight
    result = service.mark_image_result("0", True)
    assert result["image_score"] == 1 and result["success"] == 1
