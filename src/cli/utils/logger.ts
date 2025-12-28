/**
 * CLI logging utilities with consistent formatting.
 */

const RESET = "\x1b[0m";
const BOLD = "\x1b[1m";
const DIM = "\x1b[2m";
const RED = "\x1b[31m";
const GREEN = "\x1b[32m";
const YELLOW = "\x1b[33m";
const BLUE = "\x1b[34m";
const MAGENTA = "\x1b[35m";
const CYAN = "\x1b[36m";

export const logger = {
  /**
   * Logs an info message.
   */
  info(message: string): void {
    console.log(`${CYAN}info${RESET}  ${message}`);
  },

  /**
   * Logs a success message.
   */
  success(message: string): void {
    console.log(`${GREEN}done${RESET}  ${message}`);
  },

  /**
   * Logs a warning message.
   */
  warn(message: string): void {
    console.log(`${YELLOW}warn${RESET}  ${message}`);
  },

  /**
   * Logs an error message.
   */
  error(message: string): void {
    console.error(`${RED}error${RESET} ${message}`);
  },

  /**
   * Logs a step in a process.
   */
  step(message: string): void {
    console.log(`${DIM}  >${RESET} ${message}`);
  },

  /**
   * Logs a header/title.
   */
  header(message: string): void {
    console.log(`\n${BOLD}${MAGENTA}${message}${RESET}\n`);
  },

  /**
   * Logs a file path.
   */
  file(path: string): void {
    console.log(`${DIM}     ${path}${RESET}`);
  },

  /**
   * Logs raw output (no formatting).
   */
  raw(message: string): void {
    console.log(message);
  },
};
