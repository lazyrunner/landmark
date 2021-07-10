"use strict";
const sax = require("sax");
const dottie = require("dottie");
const stream = require("stream");
const endOfLine = require("os").EOL;

module.exports = function (input) {
	const comma = input.delimiter || ",";
	const saxStream = sax.createStream(true);
	const output = new stream.PassThrough();

	saxStream.on("error", err => {
		throw err;
	});

	let count = 0;
	let accepting = false;
	let currentObj;
	let pathParts = [];
	let pathPartsString;

	output.push(writeHeadersToStream(input.headerMap, comma));

	saxStream.on("opentag", function (t) {
		if (t.name === input.rootXMLElement) {
			accepting = true;
			pathParts = [];
			currentObj = {};
		} else {
			if (accepting) {
				pathParts.push(t.name);
				pathPartsString = pathParts.join(".");
			}
		}
	});

	saxStream.on("text", function (text) {
		if (accepting) {
			if (text.trim() !== "\n" && text.trim() !== "") {
				dottie.set(currentObj, pathPartsString, text);
			}
		}
	});

	saxStream.on("closetag", (tagName) => {
		if (tagName === input.rootXMLElement) {
			output.push(writeRecordToStream(currentObj, input.headerMap, comma));
			count++;
			if(count%500 == 0){
				console.log(`Inserted ${count}`)
			}
			accepting = false;
			currentObj = {};
		} else {
			pathParts.pop();
		}
	});

	saxStream.on("end", () => {
		output.push(null);
		const msg = `Finished writing records: ${count}`
		console.log(msg);
		return msg;
	});

	input.source.pipe(saxStream);

	return output;
};

function writeHeadersToStream(headerMap, comma) {
	let headerString = "";
	for (let [idx, header] of headerMap.entries()) {
		const separator = idx === headerMap.length - 1 ? endOfLine : comma;
		headerString += header[1] + separator;
	}
	return headerString;
}

function writeRecordToStream(record, headerMap, comma) {
	let recordString = "";
	for (let [idx, header] of headerMap.entries()) {
		let field= '';
		if(header.length == 4){
			if(record[header[3]]){
				field = record[header[3]][header[0]];
			}
		} else {
			if(record[header[0]]){
				field = record[header[0]];
			}
		}
		if(header.length == 5){
			if(record[header[3]]){
				field = record[header[3]][header[4]][header[0]]
			}
			
		}
		const separator = idx === headerMap.length - 1 ? endOfLine : comma;

		recordString += writeField(field, separator);
	}

	return recordString;
}

function writeField(field, separator) {
	if (!field) return separator;

	const quote = "";

	return `${quote}${field}${quote}${separator}`;
}