
import { S3Client,GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import fs from "fs";
import path from "path";
import { pipeline } from "stream/promises";
import ffmpeg from "fluent-ffmpeg";


const s3Client = new S3Client({
  region: "us-east-1",
  credentials: {
    accessKeyId: "access-key-here",
    secretAccessKey: "secret-key-here"
  },
});

const BUCKET_NAME = process.env.BUCKET_NAME;
const KEY = process.env.KEY;

// Example resolutions
const RESOLUTIONS = [
  { name: "1080p", width: 1920, height: 1080 },
  { name: "720p", width: 1280, height: 720 },
  { name: "480p", width: 854, height: 480 },
];

async function init() {
  try {
    // 1. Fetch file from S3
    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: KEY,
    });
    const result = await s3Client.send(command);
    console.log("✅ File fetched from S3");

    // 2. Save original file locally using stream (better for large files)
    const originalFilePath = "original-video.mp4";
    await pipeline(result.Body, fs.createWriteStream(originalFilePath));
    console.log("✅ Original video saved locally");

    const originalVideoPath = path.resolve(originalFilePath);

    // 3. Process each resolution in parallel
    const promises = RESOLUTIONS.map((res) => {
      const output = `video-${res.name}.mp4`;

      return new Promise((resolve, reject) => {
        ffmpeg(originalVideoPath)
          .output(output)
          .withVideoCodec("libx264")
          .withAudioCodec("aac")
          .withSize(`${res.width}x${res.height}`)
          .on("end", async () => {
            try {
              const putCommand = new PutObjectCommand({
                Bucket: "production.hehe",
                Key: output,
                Body: fs.createReadStream(output), // ✅ Upload actual file
              });
              await s3Client.send(putCommand);
              console.log(`✅ Uploaded ${output} to S3`);
              resolve(output);
            } catch (error) {
              reject(error);
            }
          })
          .on("error", (err) => {
            console.error(`❌ Error processing ${res.name}:`, err);
            reject(err);
          })
          .format("mp4")
          .run();
      });
    });

    // 4. Wait for all encodes/uploads to finish
    const outputFiles = await Promise.all(promises);
    console.log("🎯 All files processed:", outputFiles);

  } catch (err) {
    console.error("💥 Processing failed:", err);
  }
}

init();
