CREATE TABLE IF NOT EXISTS Snapshot(
    aggregateId BINARY(16) NOT NULL,
    sequenceNumber BIGINT NOT NULL,
    eventType INTEGER NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY(aggregateId)
) ENGINE = InnoDB DEFAULT CHARSET=utf8;
