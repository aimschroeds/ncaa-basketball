const express = require("express");
const bodyParser = require("body-parser");
const app = express();
const mysql = require("mysql");
const port = 8088;
require('dotenv').config();

app.use(bodyParser.urlencoded({ extended: true }));

const db = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_WEBUSER,
    password: process.env.DB_WEBUSER_PASSWORD,
    database: process.env.DB_NAME
});
// connect to database
db.connect((err) => {
    if (err) {
        throw err;
    }
    console.log("Connected to database");
});

global.db = db;

app.use(express.static('public')); // Adds public folder for static files

require("./routes/main.js")(app);

app.set("views", __dirname + "/views");
app.set("view engine", "ejs");

app.engine("html", require("ejs").renderFile);

app.listen(port, () => console.log(`Example app listening on port ${port}!`));