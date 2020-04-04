CREATE TABLE Transaction(
    aggregateId UUID NOT NULL,
    transactionId UUID NOT NULL,
    PRIMARY KEY (aggregateId, transactionId)
);