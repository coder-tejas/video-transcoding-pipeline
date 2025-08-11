import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} from "@aws-sdk/client-sqs";
import { S3Event } from "aws-lambda";
import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";
import dotenv from "dotenv";
dotenv.config();

const sqsClient = new SQSClient({
  region: "us-east-1",
  credentials: {
    accessKeyId: "access-key-here",
    secretAccessKey: "secret-key-here",
  },
});

const ecsClient = new ECSClient({
  region: "us-east-1",
  credentials: {
    accessKeyId: "access-key-here",
    secretAccessKey: "secret-key-here",
  },
});

const QUEUE_URL = "queue-url-here";

const init = async () => {
  while (true) {
    try {
      const command = new ReceiveMessageCommand({
        QueueUrl: QUEUE_URL,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 10, // Enable long polling
        VisibilityTimeout: 20, // Optional: invisibility to allow processing
      });

      const { Messages } = await sqsClient.send(command);

      if (!Messages || Messages.length === 0) {
        console.log("No messages in queue, waiting 5 seconds...");
        await new Promise((r) => setTimeout(r, 5000));
        continue;
      }

      for (const message of Messages) {
        const { MessageId, Body } = message;
        console.log("Message Received", { MessageId, Body });

        if (!Body) {
          continue;
        }

        const event = JSON.parse(Body) as S3Event; 

        if ("Service" in event && "Event" in event) {
          if (event.Event === "s3:TestEvent") {
            await sqsClient.send(
              new DeleteMessageCommand({
                QueueUrl: QUEUE_URL,
                ReceiptHandle: message.ReceiptHandle,
              })
            );
            continue;
          }
        }
        //  Spin the docker container
        for (const record of event.Records) {
          const { s3 } = record;
          const {
            bucket,
            object: { key },
          } = s3;
          //spin the docker container
          const runTaskCommand = new RunTaskCommand({
            taskDefinition:
              "arn:aws:ecs:us-east-1:244920586027:task-definition/video-transcoder:1",
            cluster: "arn:aws:ecs:us-east-1:244920586027:cluster/dev",
            launchType: "FARGATE",
            networkConfiguration: {
              awsvpcConfiguration: {
                assignPublicIp: "ENABLED",
                securityGroups: ["sg-0794f9ef94f3823ec"],
                subnets: [
                  "subnet-0158b189a61969db4",
                  "subnet-095cf32fe52e07aa6",
                  "subnet-01ed9380a336b6399",
                ],
              },
            },
            overrides: {
              containerOverrides: [
                {
                  name: "video-transcoder",
                  environment: [
                    { name: "BUCKET_NAME", value: bucket.name },
                    { name: "KEY", value: key },
                  ],
                },
              ],
            },
          });

          await ecsClient.send(runTaskCommand);
          // Delete the message after processing so it is not received again
          await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: QUEUE_URL,
              ReceiptHandle: message.ReceiptHandle,
            })
          );
        }
      }
    } catch (error) {
      console.error("Error receiving messages:", error);
      // Wait a bit before retrying to avoid rapid failure loop
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
};

init();
