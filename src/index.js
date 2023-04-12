const { QldbDriver, RetryConfig } = require("amazon-qldb-driver-nodejs");
const { Agent } = require("https");
const { insertDocument } = require("./app")
const { name: PROJECT_NAME } = require("../package.json");

const { AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY } = process.env


const createTable = async (txn, tableName) => {
    await txn.execute(`CREATE TABLE ${tableName}`);
}

const createIndex = async (txn, tableName, indexName) => {
    await txn.execute(`CREATE INDEX on ${tableName} (${indexName})`);
}

const TRANSACTION_TABLE_NAME = "accounts"
const TRANSACTION_INDEX_NAME = "account"

const initData = [
    { account: "12345", balance: 0, name: "bank" },
    { account: "6789", balance: 50, name: "test1" },
    { account: "1012", balance: 60, name: "test2" }
]

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
            await createTable(txn, TRANSACTION_TABLE_NAME);
            await createIndex(txn, TRANSACTION_TABLE_NAME, TRANSACTION_INDEX_NAME);
            await Promise.all(initData.map(({ balance, account, name }) => insertDocument(txn, TRANSACTION_TABLE_NAME, { balance, account, name })))
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