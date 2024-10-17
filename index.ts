import { parse } from "csv-parse";
import fs from "fs";
import { MySQLClient } from "./database";
// specify the path of the CSV file
const mysql = new MySQLClient();

// Create a readstream
// Parse options: delimiter and start from line 1
const TEMPLATE_1: { name: string } = {
  name: "fundamental2-email1",
};

const TEMPLATE_2: { name: string } = {
  name: "fundamental2-email2",
};

const TEMPLATE_3: { name: string } = {
  name: "fundamental2-email3",
};

await mysql.createConnection();
console.log(`[${new Date().toUTCString()}] - CONNECTED TO DATABASE`);

async function insertRec(line: number, email: string) {
  // executed for each row of data
  console.log(`[${new Date().toUTCString()}] - Line: ${line} => ${email}`);
  const hasEntryTemplate1 = await mysql
    .getEmailTemplate(email, TEMPLATE_1.name)
    .then((res) => {
      return res;
    });
  if (hasEntryTemplate1 === null) {
    await mysql
      .insertScheduledEmailTemplate(email, TEMPLATE_1.name)
      .then((res) => {
        console.log(
          `[${new Date().toUTCString()}] - ${line} => Inserted template 1 for email ${email}`
        );
      });
  }
  const hasEntryTemplate2 = await mysql
    .getEmailTemplate(email, TEMPLATE_2.name)
    .then((res) => {
      return res;
    });
  if (hasEntryTemplate2 === null) {
    await mysql
      .insertScheduledEmailTemplate(email, TEMPLATE_2.name)
      .then((res) => {
        console.log(
          `[${new Date().toUTCString()}] - ${line} => Inserted template 2 for email ${email}`
        );
      });
  }
  const hasEntryTemplate3 = await mysql
    .getEmailTemplate(email, TEMPLATE_3.name)
    .then((res) => {
      return res;
    });
  if (hasEntryTemplate3 === null) {
    await mysql
      .insertScheduledEmailTemplate(email, TEMPLATE_3.name)
      .then((res) => {
        console.log(
          `[${new Date().toUTCString()}] - ${line} => Inserted template 3 for email ${email}`
        );
      });
  }
}

async function run() {
  let line: number = 0;
  try {
    // const path = "./envio_fund2.csv";
    const path = "./envio_fund2.csv";
    const readStream = fs
      .createReadStream(path)
      .pipe(parse({ delimiter: ",", from_line: 2 }))
      .on("data", async (row) => {
        line++;
        readStream.pause();
        await insertRec(line, row);
        readStream.resume();
      })
      .on("error", async function (error) {
        // Handle the errors
        console.log(
          `[${new Date().toUTCString()}] - ERRO PROCESSING LINE ${line} => ${
            error.message
          }`
        );
      })
      .on("end", async function () {
        // executed when parsing is complete
        console.log(`[${new Date().toUTCString()}] - File read successful`);
        await mysql.disconnect();
        console.log(`[${new Date().toUTCString()}] - Connection closed`);
      });
  } catch (error) {
    console.log(
      `[${new Date().toUTCString()}] - ERRO PROCESSING LINE ${line} => ${
        error.message
      }`
    );
  }
}

await run();
