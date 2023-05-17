import { createReadStream, createWriteStream } from "fs";
import pAll from "p-all";
import { join } from "path";
import { createInterface } from "readline";
const chunkSize = 500 * 1024 * 1024;
const encoding = "utf8";

const sortAndWrite = (lines: Array<string>, chunkIndex: number): string => {
  console.log({ msg: "got sortAndWrite" });

  // можно вписать временную папку ОСи вместо partitions
  const chunkPath = join(__dirname, "partitions", `part_${chunkIndex}.txt`);
  const writeStream = createWriteStream(chunkPath);

  const sortedLines = lines.sort();
  writeStream.write(`${sortedLines}\n`);
  writeStream.close();
  console.log({ msg: "complete sortAndWrite", chunkPath });
  return chunkPath;
};

const writeCompleteFile = async (partitions: Array<string>) => {
  const writeStream = createWriteStream(join(__dirname, "total.txt"));
  const readLineInterfaces = partitions.map((partition) =>
    createInterface({
      input: createReadStream(partition, { encoding }),
    })
  );

  const heap: string[] = [];
  const rlPromises = readLineInterfaces.map((rl) => async () => {
    const { value, done } = await rl[Symbol.asyncIterator]().next();
    if (!done) {
      heap.push(value);
    }
  });
  await pAll(rlPromises, { concurrency: 5 });
  heap.sort();

  while (heap.length > 0) {
    const line = heap.shift();
    writeStream.write(`${line}\n`);

    // Добавляем следующую строку из соответствующего чанка в кучу
    const rlIndex = heap.length % readLineInterfaces.length;
    console.log({
      rlIndex,
      heap_length: heap.length,
      rli_length: readLineInterfaces.length,
    });

    const rl = readLineInterfaces[rlIndex];
    const { value, done } = await rl[Symbol.asyncIterator]().next();
    if (!done) {
      heap.push(value);
    }
    heap.sort();
  }

  // Закрываем все потоки
  for (const rl of readLineInterfaces) {
    rl.close();
  }
  writeStream.close();
  console.log({ msg: "done writing total" });
};

(() => {
  const readStream = createReadStream(join(__dirname, "../largefile.txt"), {
    encoding,
  });

  const rlInterface = createInterface({ input: readStream });
  let totalSize = 0;
  let chunkIndex = 0;
  let lines: Array<string> = [];
  const chunks: Array<string> = [];

  rlInterface.on("line", async (line) => {
    // console.log({
    //   line,
    //   length: Buffer.byteLength(line, encoding),
    //   totalSize,
    //   chunkSize,
    // });

    totalSize += Buffer.byteLength(line, encoding);
    lines.push(line);

    if (totalSize > chunkSize || chunkIndex > 4) {
      chunks.push(sortAndWrite(lines, chunkIndex));
      chunkIndex++;
      lines = [];
      totalSize = 0;
    }
  });

  rlInterface.on("close", async () => {
    console.log({ msg: "closing" });
    if (lines.length > 0) {
      chunks.push(sortAndWrite(lines, chunkIndex));
    }
    chunkIndex++;
    lines = [];
    totalSize = 0;
    await writeCompleteFile(chunks);
  });
})();
