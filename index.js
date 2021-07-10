const fs = require("fs");
const xmlcsv = require("./xmltocsv");
var parseString = require('xml2js').parseString;
const axios = require('axios');
var unzipper = require('unzipper');

async function makeAPICall() {
    let res = await axios('https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100')
    if (res.status == 200) {
        return res.data
    }
    return null;
}

async function parseXMLtoFetchLink() {
    let link = '';
    var xmlData = await makeAPICall();
    console.log("got xmlData");
    if (xmlData) {
        console.log("got xmlData");
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
        console.log(link);
        if (link) {
            axios({
                method: 'get',
                url: link,
                responseType: 'stream',
            }).then(async response => {
                 let streams = xmlcsv({
                    source: response.data.pipe(unzipper.ParseOne()),
                    rootXMLElement: "FinInstrm",
                    headerMap: [
                        ["Id", "FinInstrmGnlAttrbts.Id", "string","TermntdRcrd","FinInstrmGnlAttrbts"],
                        ["FullNm", "FinInstrmGnlAttrbts.FullNm", "string","TermntdRcrd","FinInstrmGnlAttrbts"],
                        ["ClssfctnTp", "FinInstrmGnlAttrbts.ClssfctnTp", "string","TermntdRcrd","FinInstrmGnlAttrbts"],
                        ["CmmdtyDerivInd", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "string","TermntdRcrd","FinInstrmGnlAttrbts"],
                        ["NtnlCcy", "FinInstrmGnlAttrbts.NtnlCcy", "string","TermntdRcrd","FinInstrmGnlAttrbts"],
                        ["Issr", "Issr", "string","TermntdRcrd"],
                        ["textNode", "TtlIssdNmnlAmt", "string","TermntdRcrd","DebtInstrmAttrbts","TtlIssdNmnlAmt"],
                    ]
                }).pipe(fs.createWriteStream("./files/landmarkData.csv"));
                streams.on("finish",()=>{
                    resolve('File has been Downloaded');
                });
                
            });
        } else {
            console.error("Unable to fetch link");
            reject('Unable to fetch link');
        }
    })


}

exports.handler = async (event) => {
    await downloadXMLFile();
    const response = {
        statusCode: 200,
        body: JSON.stringify('File has been saved'),
    };
    return response;
};
