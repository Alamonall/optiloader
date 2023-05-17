import { createReadStream, createWriteStream, ReadStream } from "fs";
import { join } from "path";
import { createInterface } from "readline";
import { comparator, log, memoryUsage } from "./utils";
const chunkSize = 30 * 1024 * 1024;

const sortAndWrite = (lines: Array<string>, chunkIndex: number): string => {
  // можно вписать временную папку ОСи вместо partitions
  const chunkPath = join(__dirname, "chunks", `part_${chunkIndex}.txt`);
  const writeStream = createWriteStream(chunkPath);
  const sortedLines = lines.sort(comparator).join("\n");
  writeStream.write(sortedLines);
  writeStream.close();
  return chunkPath;
};

const writeCompleteFile = async (partitions: Array<string>) => {
  log("start writing total", { partitions });
  const writeStream = createWriteStream(join(__dirname, "total.txt"));
  const readStreams: Array<ReadStream> = [];
  const readLineInterfaces = partitions.map((partition) => {
    const rs = createReadStream(partition);
    readStreams.push(rs);
    return createInterface({
      input: rs,
      crlfDelay: Infinity,
    });
  });

  memoryUsage();
  log("start creating heap", { rll: readLineInterfaces.length });
  const heap: Array<string> = [];
  for await (const rl of readLineInterfaces) {
    const { value, done } = await rl[Symbol.asyncIterator]().next();
    if (!done) {
      heap.push(value);
    }
  }
  heap.sort(comparator);

  log("start burning heap", heap);
  while (heap.length > 0) {
    const line = heap.shift();
    writeStream.write(`${line}\n`);

    // Добавляем следующую строку из соответствующего чанка в кучу
    const rlIndex = heap.length % readLineInterfaces.length;
    const rl = readLineInterfaces[rlIndex];
    const { value, done } = await rl[Symbol.asyncIterator]().next();
    if (!done) {
      heap.push(value);
    }
    heap.sort(comparator);
  }

  for (const rl of readLineInterfaces) {
    rl.close();
  }
  for (const rs of readStreams) {
    rs.close();
  }

  writeStream.close();
};

(async () => {
  log("start");
  const readStream = createReadStream(join(__dirname, "../smallfile.txt"));

  log("read stream ready");
  const rlInterface = createInterface({ input: readStream });
  let totalSize = 0;
  let chunkIndex = 0;
  let lines: Array<string> = [];
  const chunks: Array<string> = [];

  log("starting reading chunks", { totalSize });
  memoryUsage();
  rlInterface.on("line", async (line) => {
    totalSize += Buffer.byteLength(line);
    lines.push(line);

    if (totalSize > chunkSize) {
      chunks.push(sortAndWrite(lines, chunkIndex));
      chunkIndex++;
      lines = [];
      totalSize = 0;
      memoryUsage();
    }
  });

  rlInterface.on("close", async () => {
    memoryUsage();
    log("closing");
    if (lines.length > 0) {
      log("lines not null");
      chunks.push(sortAndWrite(lines, chunkIndex));
    }
    chunkIndex++;
    lines = [];
    totalSize = 0;
    await writeCompleteFile(chunks);
    memoryUsage();
  });
})();
