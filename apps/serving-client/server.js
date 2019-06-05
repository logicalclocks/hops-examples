const express = require('express')
const app = express()
const bodyParser = require('body-parser');
const request = require('request');

const argv = require('yargs')
    .usage('Usage: $0 -u [SERVING_URL] -t [INFERENCE_TOKEN]')
    .alias('t', 'token')
    .nargs('t', 1)
    .describe('t', 'Supply the inference token from Hopsworks')
    .alias('u', 'url')
    .nargs('u', 1)
    .describe('u', 'The URL of the model serving endpoint')
    .alias('p', 'port')
    .nargs('p', 1)
    .describe('p', 'The port for the web server to listen on')
    .default('p', 8080)
    .demandOption(['t', 'u'])
    .help('h')
    .alias('h', 'help')
    .argv

app.use(express.static('public'));
app.use(bodyParser.urlencoded({ extended: true }));
app.set('view engine', 'ejs')

app.get('/', function (req, res) {
  res.render('index', {loan_status_bin: null, error: null});
})

app.post('/', function (req, res) {
  let loanAmount = req.body.loan_amnt;
  let map = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7}
  let grade = map[req.body.grade];
  let annualIncome = Math.log(req.body.annual_inc);
  let interestRate = req.body.int_rate;
  let options = {
  	url: argv.u,
	method: 'POST',
	json: {signature_name: "predict", instances: [{
		loan_amnt: parseFloat(loanAmount),
		grade: parseFloat(grade),
		annual_inc_log: parseFloat(annualIncome),
		int_rate: parseFloat(interestRate)}]},
	headers: {
		'User-Agent': 'my request',
	    'Authorization': 'Bearer ' + argv.t,
	    'Content-Type': 'application/json',
	    'Accept': 'application/json'
	}};
  request(options, function (err, response, body) {
    if(err){
      console.log(err)
      res.render('index', {loan_status_bin: null, error: 'Error, please try again', error_details: null});
    } else {
      if(body.hasOwnProperty('errorCode')){
        res.render('index', {loan_status_bin: null, error: body.errorMsg, error_details: body.usrMsg == undefined ? null : body.usrMsg});
      } else {
      	let result = JSON.parse(body.replace(/\bNaN\b/g, "null"));
      	console.log(result)
      	let map = {0: 'Fully Paid', 1: 'Charged Off'}
      	result = map[result.predictions[0].classes]
        res.render('index', {loan_status_bin: result, error: null, error_details: null});
      }
    }
  });
})

app.listen(argv.p, function () {
  console.log('Lending club client app listening on ' + argv.p)
})