const AWS = require('aws-sdk');
const fs = require("fs");
const xmlcsv = require("./xmltocsv");
var parseString = require('xml2js').parseString;
const axios = require('axios');
var unzipper = require('unzipper');
const stream = require('stream');

async function makeAPICall() {
    console.log("Fetching File with DLTINS link.");
    let res = await axios('https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100')
    if (res.status == 200) {
        return res.data
    }
    return null;
}

const uploadStream = ({ Bucket, Key }) => {
    const s3 = new AWS.S3();
    const pass = new stream.PassThrough();
    return {
        writeStream: pass,
        promise: s3.upload({ Bucket, Key, Body: pass }).promise(),
    };
}

async function parseXMLtoFetchLink() {
    let link = '';
    var xmlData = await makeAPICall();
    if (xmlData) {
        console.log("Fetching Link from downloaded file.");
        return new Promise((resolve, reject) => {
            parseString(xmlData, function (err, result) {
                if (err) {
                    reject(err.message);
                }
                let results = result.response.result[0].doc[0].str;
                results.forEach(element => {
                    if (element['$'].name == 'download_link') {
                        link = element['_'];
                    }
                });
                resolve(link);
            });
        })
    } else {
        console.log("APi was unsucessful.")
        return null;
    }
}

function downloadXMLFile() {
    return new Promise(async (resolve, reject) => {
        let link = await parseXMLtoFetchLink();
        console.log('Link Fetched ', link);
        if (link) {
            console.log('Downloading file -> Unzipping -> Parsing XML -> Uploading to S3');
            axios({
                method: 'get',
                url: link,
                responseType: 'stream',
            }).then(async response => {
                const { writeStream, promise } = uploadStream({ Bucket: process.env.BUCKET, Key: process.env.FILE });
                let streams = xmlcsv({
                    source: response.data.pipe(unzipper.ParseOne()),
                    rootXMLElement: "FinInstrm",
                    delimiter: '|',
                    headerMap: [
                        ["Id", "FinInstrmGnlAttrbts.Id", "string", "TermntdRcrd", "FinInstrmGnlAttrbts"],
                        ["FullNm", "FinInstrmGnlAttrbts.FullNm", "string", "TermntdRcrd", "FinInstrmGnlAttrbts"],
                        ["ClssfctnTp", "FinInstrmGnlAttrbts.ClssfctnTp", "string", "TermntdRcrd", "FinInstrmGnlAttrbts"],
                        ["CmmdtyDerivInd", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "string", "TermntdRcrd", "FinInstrmGnlAttrbts"],
                        ["NtnlCcy", "FinInstrmGnlAttrbts.NtnlCcy", "string", "TermntdRcrd", "FinInstrmGnlAttrbts"],
                        ["Issr", "Issr", "string", "TermntdRcrd"],
                        ["TtlIssdNmnlAmt", "TtlIssdNmnlAmt", "string", "TermntdRcrd", "DebtInstrmAttrbts"],
                    ]
                }).pipe(writeStream);
                promise.then(() => {
                    console.log('Upload completed successfully');
                    resolve('File has been Downloaded');
                }).catch((err) => {
                    console.log('upload failed.', err.message);
                });

            });
        } else {
            console.error("Unable to fetch link");
            reject('Unable to fetch link');
        }
    })


}

exports.handler = async (event) => {
    try {
        await downloadXMLFile();
        const response = {
            statusCode: 200,
            body: JSON.stringify('File has been saved'),
        };
        return response;
    } catch(e){
        const response = {
            statusCode: 400,
            body: JSON.stringify('An Error has occured.'),
        };
    }
    
};

