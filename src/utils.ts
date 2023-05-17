export const comparator = (a: string, b: string) =>
  a.length < b.length ? -1 : 1;
export const memoryUsage = () => {
  const mu = process.memoryUsage();

  const field = "heapUsed";
  const gbNow = mu[field] / 1024 / 1024 / 1024;
  const gbRounded = Math.round(gbNow * 100) / 100;

  console.log({
    msg: `Heap allocated ${gbRounded} GB`,
    field: mu[field],
    gbNow: mu[field] / 1024 / 1024,
  });
};
export const log = (msg: string | number, ...rest: any) =>
  console.log({ msg, ...rest });
