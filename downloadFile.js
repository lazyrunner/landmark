var parseString = require('xml2js').parseString;
const axios = require('axios');
var unzipper = require('unzipper');
var fs = require('fs');

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

module.exports = function downloadXMLFile() {
    return new Promise(async (resolve, reject) => {
        let link = await parseXMLtoFetchLink();
        console.log(link);
        if (link) {
            axios({
                method: 'get',
                url: link,
                responseType: 'stream',
            }).then(async response => {
                var out = fs.createWriteStream('./files/source.xml');
                response.data.pipe(unzipper.ParseOne()).pipe(out);
                out.destroy();
                resolve('File has been Downloaded');
            });
        } else {
            console.error("Unable to fetch link");
            reject('Unable to fetch link');
        }
    })


}


