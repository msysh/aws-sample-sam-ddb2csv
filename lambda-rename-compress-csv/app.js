'use strict';

const stream = require('stream');

const archiver = require('archiver');

const dayjs = require("dayjs");
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const strftime = require("./dayjs-plugin-strftime");
dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(strftime);

const AWS = require("aws-sdk");
const s3 = new AWS.S3( { apiVersion: '2006-03-01'} );

const S3_DST_BUCKET = process.env.S3_BUCKET;
const S3_DST_PREFIX_FORMAT = process.env.S3_PREFIX_FORMAT;
const S3_DST_TIMEZONE_FOR_PREFIX_FORMAT = process.env.S3_TIMEZONE_FOR_PREFIX_FORMAT || 'UTC';
const S3_DST_CSV_FILE_NAME = process.env.S3_CSV_FILE_NAME;

const regexpS3Location = new RegExp('s3://([^/]+)/(.*?)/?([^/]+\.csv)$');

const UPLOAD_CONCURRENCY = 2;
const UPLOAD_PART_SIZE = 128 * 1024 * 1024;
const HIGH_WATER_MARK = UPLOAD_CONCURRENCY * UPLOAD_PART_SIZE;

exports.lambdaHandler = async (event, context, callback) => {

    const src = extractS3Location(event['queryResult']['QueryExecution']['ResultConfiguration']['OutputLocation']);

    const exportTimestamp = event['ExportTime'];
    const dst_prefix = getFormatedPrefix(exportTimestamp, S3_DST_PREFIX_FORMAT, S3_DST_TIMEZONE_FOR_PREFIX_FORMAT);

    try {
        const srcStream = s3.getObject({
            Bucket: src.bucket,
            Key: src.key
        }).createReadStream(undefined, { highWaterMark: HIGH_WATER_MARK });

        const archive = archiver('zip');
        archive.on('error', err => {
            console.error('Archive error');
            throw new Error(err);
        });

        const promise = await new Promise((resolve, reject) => {

            const passStream = new stream.PassThrough({ highWaterMark: HIGH_WATER_MARK });

            const upload = s3.upload({
                Bucket: S3_DST_BUCKET,
                Key: `${dst_prefix}/${S3_DST_CSV_FILE_NAME}.gz`,
                Body: passStream,
                ContentType: 'application/gz'
            }, {
                partSize: UPLOAD_PART_SIZE,
                queueSize: UPLOAD_CONCURRENCY
            }, (err, data) => {
                if (err){
                    console.error('Error uploading to S3:');
                    console.error(err);
                    reject(err);
                }
                else{
                    console.info('Success uploading to S3:');
                    console.info(data);
                    resolve(data);
                }
            });
            upload.on('httpUploadProgress', (progress) => {
                console.debug(`loaded: ${progress.loaded} / total: ${progress.total} / part: ${progress.part} / key: ${progress.key}`);
            });

            archive.pipe(passStream);
            archive.append(srcStream, { name: S3_DST_CSV_FILE_NAME });
            archive.finalize();

            upload.on('close', (arg) => {console.log('upload on close'); console.log(arg); resolve(arg)});
            upload.on('end', (arg) => {console.log('upload on end'); console.log(arg); resolve(arg)});
            upload.on('error', (arg) => {console.log('upload on error'); console.log(arg); reject(arg)});

        }).then((data) => {
            console.info('Start deleting source files:');
            return s3.deleteObjects({
                    Bucket: src.bucket,
                    Delete: {
                        Objects: [
                            { Key: src.key },
                            { Key: `${src.key}.metadata` }
                        ]
                    }
                }, (err, data) => {
                    if (err){
                        console.error('Error deleting source file:');
                        console.error(err, err.stack);
                    }
                    else{
                        console.info('Success deleting source file:');
                        console.info(data);
                    }
                }).promise();
        }).catch(err => { throw new Error(err) });

        return promise;
    } catch (err) {
        console.error(err);
        return err;
    }
};

const extractS3Location = (s3Location) => {
    const result = s3Location.match(regexpS3Location);
    if (!result){
        throw new Error(`Can not parse s3 location: ${s3Location}`);
    }

    return {
        bucket: result[1],
        key: `${result[2]}/${result[3]}`,
        filename: result[3]
    };
};

const getFormatedPrefix = (timestamp, prefixFormat, timezone) => {
    let d = dayjs(new Date(parseInt(timestamp, 10) * 1000)).tz(timezone);
    return d.strftime(prefixFormat);
};
