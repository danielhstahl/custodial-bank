const { QldbDriver, RetryConfig } = require("amazon-qldb-driver-nodejs");
const { Agent } = require("https");
const { name: PROJECT_NAME } = require("../package.json");

const { AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY } = process.env

const insertDocument = async (txn, tableName, record) => {
    await txn.execute(`INSERT INTO ${tableName} ?`, record);
}
module.exports = { insertDocument }
const updateDocument = async (txn, tableName, indexName, indexValue, fieldName, fieldValue) => {
    await txn.execute(`UPDATE ${tableName} SET ${fieldName} = ? WHERE ${indexName} = ?`, fieldValue, indexValue);
}

const TRANSACTION_TABLE_NAME = "accounts"
const TRANSACTION_INDEX_NAME = "account"

const createAccountUpdatePayload = (accountNumber, transactionType, amount, previousBalance) => {
    const currentBalance = previousBalance ? previousBalance + amount : amount //amount can be negative
    return { [TRANSACTION_INDEX_NAME]: accountNumber, type: transactionType, amount, balance: currentBalance }
}

const getRecord = async (txn, tableName, indexName, indexValue, ...rest) => {
    return (await txn.execute(`SELECT ${indexName}, ${rest.join(",")} FROM ${tableName} WHERE ${indexName} = ? `, indexValue)).getResultList();
}

const getAccount = async (txn, accountNumber) => {
    return (await getRecord(txn, TRANSACTION_TABLE_NAME, TRANSACTION_INDEX_NAME, accountNumber, "balance"))[0]
}

const updateAccount = async (txn, accountNumber, balance) => {
    return await updateDocument(txn, TRANSACTION_TABLE_NAME, TRANSACTION_INDEX_NAME, accountNumber, "balance", balance)
}

const createTransaction = async (txn, amount, sendingAccount, receivingAccount, bankAccount, transactionFee) => {
    const { balance: sendingBalance } = await getAccount(txn, sendingAccount)
    const totalAmount = amount * (1.0 + transactionFee)
    if (sendingBalance - totalAmount > 0) {
        const sendingTransaction = createAccountUpdatePayload(sendingAccount, "sending", -amount, sendingBalance)
        const fee = totalAmount - amount
        await updateAccount(txn, sendingAccount, sendingTransaction.balance)
        const { balance: receivingBalance } = getAccount(txn, receivingAccount)
        const receivingTransaction = createAccountUpdatePayload(receivingAccount, "receiving", amount, receivingBalance)
        await updateAccount(txn, receivingAccount, receivingTransaction.balance)
        const { balance: bankBalance } = getAccount(txn, bankAccount)
        const bankTransaction = createAccountUpdatePayload(receivingAccount, "receiving", fee, bankBalance)
        await updateAccount(txn, bankAccount, bankTransaction.balance)
    }
}

const init = async () => {
    const maxConcurrentTransactions = 10;
    const retryLimit = 4;
    const config = {
        region: AWS_REGION,
        credentials: {
            accessKeyId: AWS_ACCESS_KEY_ID,
            secretAccessKey: AWS_SECRET_ACCESS_KEY
        }
    }

    const agentForQldb = new Agent({
        maxSockets: maxConcurrentTransactions
    });

    const lowLevelClientHttpOptions = {
        httpAgent: agentForQldb
    }

    // Use driver's default backoff function for this example (no second parameter provided to RetryConfig)
    const retryConfig = new RetryConfig(retryLimit);
    const driver = new QldbDriver("transactions-ledger", config, lowLevelClientHttpOptions, maxConcurrentTransactions, retryConfig);
    try {
        await driver.executeLambda(async (txn) => {
            await createTransaction(txn, 20, "6789", "1012", "12345", 0.01)
            console.log(await getAccount(txn, "6789"))
            console.log(await getAccount(txn, "1012"))
            //await insertDocument(TRANSACTION_TABLE_NAME, createTransaction("accountnumber1", "funding", 500, null))// { [TRANSACTION_INDEX_NAME]: "accountnumber1", type: "funding", amount: 500, balance: 500 })
        });

    }
    catch (e) {
        console.log(e)
    }
    finally {
        driver.close();
    }

}

init().then(console.log)

