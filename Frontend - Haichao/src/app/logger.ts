type LogArgs = unknown[];

type Logger = {
  log: (...args: LogArgs) => void;
  debug: (...args: LogArgs) => void;
  warn: (...args: LogArgs) => void;
  error: (...args: LogArgs) => void;
};

const noop = () => {};

export const devLogger: Logger = import.meta.env.DEV
  ? {
      log: (...args) => console.log(...args),
      debug: (...args) => console.debug(...args),
      warn: (...args) => console.warn(...args),
      error: (...args) => console.error(...args),
    }
  : {
      log: noop,
      debug: noop,
      warn: noop,
      error: noop,
    };
