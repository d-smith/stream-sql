# stream-sql

## tools

Install python via miniconda - see [this page](https://docs.conda.io/en/latest/miniconda.html)

Once conda is installed, install jupyter-lab as per [here](https://jupyter.org/install)

Install the AWS python SDK:

```
pip install boto3
```

## by hand guide

[SQL developer guide](https://docs.aws.amazon.com/kinesisanalytics/latest/dev/what-is.html)

* In console create an app
* Add an input stream, e.g. s1
    * For schema discovery post some data to the stream - see below
* Add SQL
* Hook up destination stream

Here's the app deets

```console
{
    "ApplicationDetail": {
        "ApplicationARN": "arn:aws:kinesisanalytics:us-east-1:123412341234:application/a1",
        "ApplicationName": "a1",
        "RuntimeEnvironment": "SQL-1_0",
        "ApplicationStatus": "RUNNING",
        "ApplicationVersionId": 5,
        "CreateTimestamp": "2021-04-15T14:43:56-07:00",
        "LastUpdateTimestamp": "2021-04-15T15:13:05-07:00",
        "ApplicationConfigurationDescription": {
            "SqlApplicationConfigurationDescription": {
                "InputDescriptions": [
                    {
                        "InputId": "2.1",
                        "NamePrefix": "SOURCE_SQL_STREAM",
                        "InAppStreamNames": [
                            "SOURCE_SQL_STREAM_001"
                        ],
                        "KinesisStreamsInputDescription": {
                            "ResourceARN": "arn:aws:kinesis:us-east-1:123412341234:stream/s1",
                            "RoleARN": "arn:aws:iam::123412341234:role/service-role/kinesis-analytics-a1-us-east-1"
                        },
                        "InputSchema": {
                            "RecordFormat": {
                                "RecordFormatType": "JSON",
                                "MappingParameters": {
                                    "JSONMappingParameters": {
                                        "RecordRowPath": "$"
                                    }
                                }
                            },
                            "RecordEncoding": "UTF-8",
                            "RecordColumns": [
                                {
                                    "Name": "specversion",
                                    "Mapping": "$.specversion",
                                    "SqlType": "DECIMAL(1,1)"
                                },
                                {
                                    "Name": "type",
                                    "Mapping": "$.type",
                                    "SqlType": "VARCHAR(32)"
                                },
                                {
                                    "Name": "source",
                                    "Mapping": "$.source",
                                    "SqlType": "VARCHAR(64)"
                                },
                                {
                                    "Name": "subject",
                                    "Mapping": "$.subject",
                                    "SqlType": "INTEGER"
                                },
                                {
                                    "Name": "id",
                                    "Mapping": "$.id",
                                    "SqlType": "VARCHAR(16)"
                                },
                                {
                                    "Name": "COL_time",
                                    "Mapping": "$.time",
                                    "SqlType": "VARCHAR(32)"
                                },
                                {
                                    "Name": "thing1",
                                    "Mapping": "$.thing1",
                                    "SqlType": "VARCHAR(8)"
                                },
                                {
                                    "Name": "COL_xstuff",
                                    "Mapping": "$.x-stuff",
                                    "SqlType": "INTEGER"
                                },
                                {
                                    "Name": "datacontenttype",
                                    "Mapping": "$.datacontenttype",
                                    "SqlType": "VARCHAR(8)"
                                },
                                {
                                    "Name": "data",
                                    "Mapping": "$.data",
                                    "SqlType": "VARCHAR(32)"
                                },
                                {
                                    "Name": "comexampleextension1",
                                    "Mapping": "$.comexampleextension1",
                                    "SqlType": "VARCHAR(8)"
                                },
                                {
                                    "Name": "comexampleothervalue",
                                    "Mapping": "$.comexampleothervalue",
                                    "SqlType": "INTEGER"
                                }
                            ]
                        },
                        "InputParallelism": {
                            "Count": 1
                        },
                        "InputStartingPositionConfiguration": {
                            "InputStartingPosition": "NOW"
                        }
                    }
                ],
                "OutputDescriptions": [
                    {
                        "OutputId": "5.1",
                        "Name": "DESTINATION_SQL_STREAM",
                        "KinesisStreamsOutputDescription": {
                            "ResourceARN": "arn:aws:kinesis:us-east-1:123412341234:stream/s2",
                            "RoleARN": "arn:aws:iam::123412341234:role/service-role/kinesis-analytics-a1-us-east-1"
                        },
                        "DestinationSchema": {
                            "RecordFormatType": "CSV"
                        }
                    }
                ]
            },
            "ApplicationCodeConfigurationDescription": {
                "CodeContentType": "PLAINTEXT",
                "CodeContentDescription": {
                    "TextContent": "/**\n * Welcome to the SQL editor\n * =========================\n * \n * The SQL code you write here will continuously transform your streaming data\n * when your application is running.\n *\n * Get started by clicking \"Add SQL from templates\" or pull up the\n * documentation and start writing your own custom queries.\n */\n\nCREATE OR REPLACE STREAM \"DESTINATION_SQL_STREAM\" (type varchar(32), event_count integer);\nCREATE OR REPLACE  PUMP \"STREAM_PUMP\" AS INSERT INTO \"DESTINATION_SQL_STREAM\"\nSELECT STREAM \"type\", COUNT(*) AS \"event_count\"\nFROM \"SOURCE_SQL_STREAM_001\"\n-- Uses a 10-second tumbling time window\nGROUP BY \"type\", FLOOR((\"SOURCE_SQL_STREAM_001\".ROWTIME - TIMESTAMP '1970-01-01 00:00:00') SECOND / 10 TO SECOND);\n"
                }
            }
        }
    }
}
```


exemplar

CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (type varchar(32), event_count integer);
CREATE OR REPLACE  PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
SELECT STREAM "type", COUNT(*) AS "event_count"
FROM "SOURCE_SQL_STREAM_001"
-- Uses a 10-second tumbling time window
GROUP BY "type", FLOOR(("SOURCE_SQL_STREAM_001".ROWTIME - TIMESTAMP '1970-01-01 00:00:00') SECOND / 10 TO SECOND);




aws kinesis put-record --stream-name s1 --partition-key x --data ewogICAgInNwZWN2ZXJzaW9uIiA6ICIxLjAiLAogICAgInR5cGUiIDogImNvbS5naXRodWIucHVsbF9yZXF1ZXN0Lm9wZW5lZCIsCiAgICAic291cmNlIiA6ICJodHRwczovL2dpdGh1Yi5jb20vY2xvdWRldmVudHMvc3BlYy9wdWxsIiwKICAgICJzdWJqZWN0IiA6ICIxMjMiLAogICAgImlkIiA6ICJBMjM0LTEyMzQtMTIzNCIsCiAgICAidGltZSIgOiAiMjAxOC0wNC0wNVQxNzozMTowMFoiLAogICAgImNvbWV4YW1wbGVleHRlbnNpb24xIiA6ICJ2YWx1ZSIsCiAgICAiY29tZXhhbXBsZW90aGVydmFsdWUiIDogNSwKICAgICJkYXRhY29udGVudHR5cGUiIDogInRleHQveG1sIiwKICAgICJkYXRhIiA6ICI8bXVjaCB3b3c9XCJ4bWxcIi8+Igp9

aws kinesis put-record --stream-name s1 --partition-key y --data ewogICAgInNwZWN2ZXJzaW9uIiA6ICIxLjAiLAogICAgInR5cGUiIDogImNvbS5naXRodWIucHVsbF9yZXF1ZXN0Lm1lcmdlZCIsCiAgICAic291cmNlIiA6ICJodHRwczovL2dpdGh1Yi5jb20vY2xvdWRldmVudHMvc3BlYy9wdWxsIiwKICAgICJzdWJqZWN0IiA6ICIxMjMiLAogICAgImlkIiA6ICJBMjM0LTEyMzQtMTIzNSIsCiAgICAidGltZSIgOiAiMjAxOC0wNC0wNVQxNzozMjowMFoiLAogICAgInRoaW5nMSIgOiAidmFsdWUiLAogICAgIngtc3R1ZmYiIDogNSwKICAgICJkYXRhY29udGVudHR5cGUiIDogInRleHQveG1sIiwKICAgICJkYXRhIiA6ICI8bXVjaCB3b3c9XCJ4bWxcIi8+Igp9

Read from the output stream

aws kinesis describe-stream --stream-name s2

Note the 

aws kinesis get-shard-iterator --stream-name s2 --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON


aws kinesis get-records --shard-iterator "AAAAAAAAAAGMLgBJT00vJcfFKgHUeZVs7Z/U/wTduXIPsug4kQAhGT3tCDkJ2HqI4xPDlPRsODU3K9nEw9uVmd2y730hJnivA4FgJQ1q9kiFJpM/tKLOIIkt5tIXw+5s/wTsw9qjvRr8LBT0Zdc50jLflKEkVxoWFUt+V4GUsWgjL6t0AoGojzl2Okh+2q7Pw7ubaBtHgpbDagggE80eJdr7YLT2oQEP"
{
    "Records": [
        {
            "SequenceNumber": "49617366210060608787406820082816642490687746193066295298",
            "ApproximateArrivalTimestamp": "2021-04-15T15:17:20.160000-07:00",
            "Data": "Y29tLmdpdGh1Yi5wdWxsX3JlcXVlc3Qub3BlbmVkLDEK",
            "PartitionKey": "鱫斧캨鎇ꗦ񸁁ᵭ⸅誁阧ꬖ봇𦂗ֶ莊ʍꑍ프㊹䪁ꠙ鍈솂랉"
        },
        {
            "SequenceNumber": "49617366210060608787406820082817851416507361440716292098",
            "ApproximateArrivalTimestamp": "2021-04-15T15:17:30.067000-07:00",
            "Data": "Y29tLmdpdGh1Yi5wdWxsX3JlcXVlc3QubWVyZ2VkLDIK",
            "PartitionKey": "栆텑𠒄뗂໭嶠安퀎赗轥君纜ꌈ힒᳽᠌ⲻ쥹䑭ڬ헼틿꨿䆄屵"
        },
        {
            "SequenceNumber": "49617366210060608787406820082819060342326976069890998274",
            "ApproximateArrivalTimestamp": "2021-04-15T15:17:30.069000-07:00",
            "Data": "Y29tLmdpdGh1Yi5wdWxsX3JlcXVlc3Qub3BlbmVkLDEK",
            "PartitionKey": "掴輧񞡌廎﹒嚆뀡樾鞐㪏魼蔰皶ဢꋇ냱ᒱᬐ삳賞뻹塎悂擋쿴뤻㉢쯤"
        }
    ],
    "NextShardIterator": "AAAAAAAAAAEQxM+nkwBmtNCNz1IUE5YGl+5AMQgRxocfPdMRx1h86eNpOKrQ7VO2zmGyvjMVCn5VJrREnihJJnltrYS4QoqXyucLTjjp2QIq47HcKnbbGkium9qAEVM91OlBtUHr/g841kTUsYbZwPSWKEfNjBJ57vvO1Qha4qO2xa/CtvswEhyHQegMC8mapPCKv9MU9jqvaA1Dt0eSJv/LOzOw5F3z",
    "MillisBehindLatest": 0
}

echo "Y29tLmdpdGh1Yi5wdWxsX3JlcXVlc3Qub3BlbmVkLDEK"|base64 --decode
com.github.pull_request.opened,1



com.github.pull_request.merged,2



