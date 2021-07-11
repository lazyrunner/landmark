const readline = require('readline');
const AWS = require('aws-sdk');
const columnMap = {
    "FinInstrmGnlAttrbts.FullNm": 1,
    "FinInstrmGnlAttrbts.ClssfctnTp": 2,
    "TtlIssdNmnlAmt": 6
}
const s3 = new AWS.S3();

exports.handler = async (event) => {
    let filter = {};
    if (event.filter) {
        filter = event.filter;
    }
    if (event.body !== null && event.body !== undefined) {
        let body = event.body;
        console.log(body);
        filter = body.filter;
    }
    var sum = 0;
    const obj = s3.getObject({ Bucket: process.env.BUCKET, Key: process.env.FILE });
    const s3Stream = obj.createReadStream();
    const readInterface = readline.createInterface({
        input: s3Stream
    });

    let streamDataLineByLine = new Promise((resolve, reject) => {

        readInterface.on('line', function (line) {
            var columns = line.split('|');
            if (filter["FinInstrmGnlAttrbts.FullNm"] && filter["FinInstrmGnlAttrbts.ClssfctnTp"]) {
                if (columns[columnMap["FinInstrmGnlAttrbts.FullNm"]] == filter["FinInstrmGnlAttrbts.FullNm"] &&
                    columns[columnMap["FinInstrmGnlAttrbts.ClssfctnTp"]] == filter["FinInstrmGnlAttrbts.ClssfctnTp"]) {
                    sum += parseInt(columns[columnMap["TtlIssdNmnlAmt"]]) ? parseInt(columns[columnMap["TtlIssdNmnlAmt"]]) : 0;
                }
            } else if (filter["FinInstrmGnlAttrbts.FullNm"] && !filter["FinInstrmGnlAttrbts.ClssfctnTp"]) {
                if (columns[columnMap["FinInstrmGnlAttrbts.FullNm"]] == filter["FinInstrmGnlAttrbts.FullNm"]) {
                    sum += parseInt(columns[columnMap["TtlIssdNmnlAmt"]]) ? parseInt(columns[columnMap["TtlIssdNmnlAmt"]]) : 0;
                }
            } else if (!filter["FinInstrmGnlAttrbts.FullNm"] && filter["FinInstrmGnlAttrbts.ClssfctnTp"]) {
                if (columns[columnMap["FinInstrmGnlAttrbts.ClssfctnTp"]] == filter["FinInstrmGnlAttrbts.ClssfctnTp"]) {
                    sum += parseInt(columns[columnMap["TtlIssdNmnlAmt"]]) ? parseInt(columns[columnMap["TtlIssdNmnlAmt"]]) : 0;
                }
            } else {
                sum += parseInt(columns[columnMap["TtlIssdNmnlAmt"]]) ? parseInt(columns[columnMap["TtlIssdNmnlAmt"]]) : 0;
            }

        }).on('close', function () {
            console.log('sum', sum);
            const response = {
                statusCode: 200,
                body: JSON.stringify({ sum, filter }),
            };
            resolve(response);
        });
    });
    try {
        let resp = await streamDataLineByLine;
        return resp;
    }
    catch (err) {
        return {
            statusCode: 200,
            message: 'An Error has occured.'
        }
    }

};
