CREATE TABLE Event(
    aggregateId UUID NOT NULL,
    sequenceNumber BIGINT NOT NULL,
    transactionId UUID NOT NULL,
    eventType INTEGER NOT NULL,
    payload BYTEA NOT NULL,
    PRIMARY KEY (aggregateId, sequenceNumber)
);

CREATE INDEX idx_transaction ON Event (aggregateId, transactionId);