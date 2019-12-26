CREATE TABLE IF NOT EXISTS Event(
    aggregateId BINARY(16) NOT NULL,
    sequenceNumber BIGINT NOT NULL,
    transactionId BINARY(16) NOT NULL,
    eventType INTEGER NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (aggregateId, sequenceNumber)
) ENGINE = InnoDB DEFAULT CHARSET=utf8;
