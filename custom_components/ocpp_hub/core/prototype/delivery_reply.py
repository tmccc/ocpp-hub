"""One actually received cloud reply awaiting its exact local journal transaction."""

from dataclasses import dataclass, field
from typing import Any

from ..frames import Frame, parse_frame
from .auth_info import AuthorizationObservation, validate_info
from .managed_start_delivery import ATTEMPTS, RETRIES
from .managed_stop_delivery import approved_stop_metadata
from .policy import DeniedError
from .store import Delivery, Journal, RecoveryRequiredError, encoded


# StopTransaction confirms delivery with an empty object or optional authorization metadata.
def stop_result_shape_supported(result: dict[str, Any]) -> bool:
    return set(result) <= {"idTagInfo"}


@dataclass(frozen=True)
class DeliveryReply:
    """The serialized delivery loop retains one reply, never an inferred remote outcome."""

    token: str
    action: str
    payload: str
    transaction: int | None
    answer: Frame
    authorization: AuthorizationObservation | None = None
    restored: bool = field(default=False, compare=False)

    # Capture immutable event identity before any reply-dependent database operation can fail.
    @classmethod
    def received(
        cls, item: Delivery, answer: Frame, authorization: AuthorizationObservation | None = None
    ) -> DeliveryReply:
        return cls(item.token, item.action, encoded(item.payload), item.transaction, answer, authorization)

    # Restart can use an actually saved response, including a denial, without inventing acceptance or retransmitting.
    @classmethod
    def restore(cls, journal: Journal, item: Delivery) -> DeliveryReply | None:
        row = journal.db.execute(
            "SELECT delivery_replies.*, sequence, observed FROM delivery_replies "
            "LEFT JOIN delivery_reply_authorization USING(token) WHERE token=?",
            (item.token,),
        ).fetchone()
        if row is None:
            return None
        answer = parse_frame(row["answer"])
        if (
            answer is None
            or type(answer[0]) is not int
            or answer[0] not in (3, 4)
            or answer[1] != item.token
            or answer[0] == 3
            and (len(answer) != 3 or not isinstance(answer[2], dict))
            or answer[0] == 4
            and (
                len(answer) != 5
                or not isinstance(answer[2], str)
                or not isinstance(answer[3], str)
                or not isinstance(answer[4], dict)
            )
        ):
            raise RecoveryRequiredError("saved cloud reply is not a correlated result")
        authorization = (
            AuthorizationObservation(row["sequence"], row["observed"]) if row["sequence"] is not None else None
        )
        reply = cls(row["token"], row["action"], row["payload"], row["local"], answer, authorization, restored=True)
        reply.require(item)
        return reply

    # Save the received outcome before ACK writes; removal shares the actual dequeue or quarantine transaction.
    def save(self, journal: Journal, item: Delivery) -> None:
        existing = self.restore(journal, item)
        if existing is not None:
            if existing != self:
                raise RecoveryRequiredError("saved cloud outcome changed before settlement")
            return
        with journal.atomic():
            head = journal.next_delivery()
            if head is None:
                raise RecoveryRequiredError("received cloud reply has no retained delivery")
            self.require(head)
            journal.db.execute(
                "INSERT INTO delivery_replies VALUES(?,?,?,?,?)",
                (self.token, self.action, self.payload, self.transaction, encoded(self.answer)),
            )
            if self.authorization is not None:
                journal.db.execute(
                    "INSERT INTO delivery_reply_authorization VALUES(?,?,?)",
                    (self.token, self.authorization.sequence, self.authorization.observed),
                )

    # A repaired database may retry only the exact received reply against its original sent head.
    def require(self, item: Delivery) -> None:
        sent = item.state == "inflight" or item.action == "StartTransaction" and item.state == "ambiguous"
        if not sent or (item.token, item.action, encoded(item.payload), item.transaction) != (
            self.token,
            self.action,
            self.payload,
            self.transaction,
        ):
            raise RecoveryRequiredError("received cloud reply no longer identifies the exact sent delivery")

    # A known accepted mapping or Stop ACK needs a local commit retry; denial and invalid outcomes retain quarantine.
    def storage_only(self, journal: Journal, item: Delivery | None = None) -> bool:
        if self.action not in ("StartTransaction", "StopTransaction"):
            return True
        if self.answer[0] != 3:
            if self.action != "StartTransaction":
                return False
            errors = journal.get(RETRIES, {}).get(self.token, {}).get("errors", 0)
            return type(errors) is int and 0 <= errors < ATTEMPTS - 1
        result = self.answer[2]
        if self.action == "StopTransaction" and not stop_result_shape_supported(result):
            return False
        try:
            if "idTagInfo" in result:
                validate_info(result["idTagInfo"])
        except DeniedError:
            return (
                self.action == "StopTransaction" and item is not None and approved_stop_metadata(journal, item, result)
            )
        return self.action == "StopTransaction" or (
            type(result.get("transactionId")) is int
            and 0 <= result["transactionId"] < 2**31
            and isinstance(result.get("idTagInfo"), dict)
            and result["idTagInfo"].get("status") == "Accepted"
        )
