const parse = require("csv-parse");
const AWS = require("aws-sdk");
const fs = require('fs');

AWS.config.update({region: 'eu-west-1'}); // Change Region Here
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const csvWriter = createCsvWriter({
    path: 'output.csv',
    header: [
        {id: 'accountId', title: 'Account ID'},
        {id: 'dbb', title: 'DB Balance'},
        {id: 'csvb', title: 'CSV Balance'},
    ]
});
 

var dynamoDB = new AWS.DynamoDB.DocumentClient();
let data = fs.readFileSync("run-1594171675835-part-r-00000 (1).csv");
function compare(accountId, row){
    return new Promise((resolve, reject) => {
        if(accountId == ''){
            resolve(0);
            return;
        }
        delete row.accountId;
        let params = {
            TableName: 'pyypl-accounts-table-AccountsTable-O3WIR47YEU2H',
            FilterExpression: "#aid = :accountId",
            ExpressionAttributeNames:{
                "#aid": "accountId"
            },
            ExpressionAttributeValues: {
                ":accountId": accountId
            }
        };
        console.log(params)
        dynamoDB.scan(params, async(err, data) => {
            if(err){
                console.log(err);
            }
            let item = data ? data.Items[0] : null;
            
            if(item){
                let balances = item.balances;
                if(typeof item.balances == 'string'){
                    let balancesStr = item.balances.replace(/\'/g, "\"");
                    balances = JSON.parse(balancesStr);
                }
                let isSame = true;
                if(balances){
                    for(const k in row){
                        if(balances[k]){
                            if(row[k] != balances[k]){
                                isSame = false;
                            }
                        }
                    }
                    if(!isSame){
                        let data = [ {accountId, 
                                dbb: JSON.stringify(balances),
                                csvb: JSON.stringify(row)}
                        ]
                        await csvWriter.writeRecords(data);
                    }
                }
                resolve(0)
            }
        })
    })
}

parse(data, {
    columns: true
}, (err, output) => {
    let outputs = output.map((row) => {
        return compare(row.accountId, row)
    })
    Promise.all(outputs).then((data) => {
        console.log(`Report has been saved in ${__dirname}/output.csv`)
    })
    
})
