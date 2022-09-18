# Assumptions

1. Withdrawl cannot be a subject of dispute, thus nor resolve and chargeback. Only deposit can be da subject of dispute. This is because given data format does not allow withdrawal for dispute
2. When accountering errors with given input data, engine will stop if the errors are related to unrecoverable errors such as undefined action type, number cannot be paresd and etc; If the errors are logical errors such as duplicated transaction id, engine will continue with only simply logging the error.
