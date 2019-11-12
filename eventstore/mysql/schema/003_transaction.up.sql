CREATE TABLE IF NOT EXISTS Transaction(
    aggregateId BINARY(16) NOT NULL,
    transactionId BINARY(16) NOT NULL,
    PRIMARY KEY (aggregateId, transactionId)
) ENGINE = InnoDB DEFAULT CHARSET=utf8;