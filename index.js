var AWS = require('aws-sdk');
var glue = new AWS.Glue();
var lambda = new AWS.Lambda();

const { Pool } = require('pg');


var db_host = process.env.db_host;
var db_user = process.env.db_user;
var db_pass = process.env.db_pass;
var db_name = process.env.db_name;
var db_port = process.env.db_port;
var db_schema = process.env.db_schema;

const connectionString = 'postgresql://' + db_user + ':' + db_pass + '@' + db_host + ':' + db_port + '/' + db_name;

const pool = new Pool({
    connectionString: connectionString,
});

exports.handler = async (event) => {
    
    var business_entity;
    const respSelect = await pool.query('SELECT distinct business_entity from ' + db_schema + '.vehicle_data_processing');
    for (const key of respSelect.rows) {
        console.log("business_entity **" + key['business_entity']);
        business_entity = key['business_entity'];
        break;
    }
    console.log("busi_outside " + business_entity);
    var currentdate = new Date();
    var formattedDate = currentdate.getFullYear().toString() + currentdate.getMonth() + 1 + (currentdate.getDate()) + currentdate.getHours() + currentdate.getMinutes() + currentdate.getSeconds() + currentdate.getMilliseconds();



    //Insert record APV_IVE_JOBS_PROCESSING_STATUS table

    console.log("in insert ** " + business_entity + "pool **** " + pool);
    var timestamp = currentdate.getTime();
    console.log("currentdate" + currentdate + " formattedDate  " + formattedDate);

    const sql = "INSERT INTO " + db_schema + ".APV_IVE_JOBS_PROCESSING_STATUS (job_id,job_name,start_time,status,business_entity) VALUES ($1,$2,$3,$4,$5)";
    const values = ['MoveToProcessing_'.concat(formattedDate), 'MoveToProcessing', currentdate, 'InProgress', business_entity];

    console.log('testt' + values);

    const resp = await pool.query(sql, values);

    if (resp.err) { console.log('error'); }
    else {
        console.log('fetched response');
        const resultString = JSON.stringify(resp);
        console.log("Insert into status table " + resultString);
    }

    //Insert record VEHICLE_DATA_REQUEST_INFO table 

    var req_id = formattedDate + 'W' + business_entity + '000000';
    var req_user = '00000';
    // var req_date = formattedDate;
    var req_type = 'Weekly';
    var req_status = 'Started';

    const sqlReqInfo = "INSERT INTO apv_ive.VEHICLE_DATA_REQUEST_INFO(Request_Id,Requested_User,Requested_date,Request_Type,Request_Status,Business_Entity) VALUES ($1,$2,$3,$4,$5,$6)";
    const valuesReqInfo = [req_id, req_user, currentdate, req_type, req_status, business_entity];

    console.log("valreqinfo " + valuesReqInfo);

    const respReqInfo = await pool.query(sqlReqInfo, valuesReqInfo);
    const resultStringReqInfo = JSON.stringify(respReqInfo);
    console.log("Insert into reqInfo " + resultStringReqInfo)

    /** Calling glue job to insert req_details table **/

    var params = {
        JobName: 'apv_insert_vehicle_data_request_detail',
        Arguments: { '--business_entity': business_entity }
    };
    console.log("before calling glue - bus-entity " + business_entity);
    //Invoke job run
    await glue.startJobRun(params, function (err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else
            console.log(data);         // successful response
    });





    /** Invoke generate nqc request file **/

    var paramsLambda = {
        FunctionName: 'ive_insert_request_info', // the lambda function we are going to invoke
        InvocationType: 'RequestResponse',
        LogType: 'Tail',
    };

    lambda.invoke(paramsLambda, function (err, data) {
        if (err) {
            context.fail(err);
        } else {
            context.succeed('ive-dev-vehicle-data-request-file successful');
        }
    })

    const response = {
        statusCode: 200,
        body: JSON.stringify('Job 3 executed successfully!'),
    };
    return response;
};