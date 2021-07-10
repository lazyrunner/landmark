const fs = require("fs");
const xmlcsv = require("./xmltocsv");
const downloadXMLFile = require("./downloadFile");
 
function convertXMLStreamtoCSV() {
    xmlcsv({
        source: fs.createReadStream("./files/source.xml"),
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
    
}

exports.handler = async (event) => {
    test();
    const response = {
        statusCode: 200,
        body: JSON.stringify('File has been saved'),
    };
    return response;
};

async function test(){
    let x = await downloadXMLFile();
    console.log('here',x);
    if(x){
        console.log('here2',x);
        convertXMLStreamtoCSV();
    }
    // fs.copyFile('./files/source.xml', './files/source2.xml', () => {
    //     console.log("\nFile copied!\n");
    //     // convertXMLStreamtoCSV();
    //   });
    
   
}

test();

