CREATE TABLE Snapshot(
    aggregateId UUID NOT NULL,
    sequenceNumber BIGINT NOT NULL,
    eventType INTEGER NOT NULL,
    payload BYTEA NOT NULL,
    PRIMARY KEY(aggregateId)
);