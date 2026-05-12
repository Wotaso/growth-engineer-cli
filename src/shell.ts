import { spawnSync } from 'node:child_process';

export type CommandResult = {
  ok: boolean;
  code: number | null;
  stdout: string;
  stderr: string;
  timedOut: boolean;
};

export const runCommand = (
  command: string,
  args: string[],
  options?: { cwd?: string; input?: string; timeoutMs?: number },
): CommandResult => {
  const result = spawnSync(command, args, {
    cwd: options?.cwd,
    encoding: 'utf8',
    input: options?.input,
    timeout: options?.timeoutMs,
  });

  const timedOut = (result.error as { code?: string } | undefined)?.code === 'ETIMEDOUT';

  return {
    ok: !result.error && result.status === 0,
    code: result.status,
    stdout: result.stdout ?? '',
    stderr: result.stderr ?? '',
    timedOut,
  };
};

export const runCommandInherited = (
  command: string,
  args: string[],
  options?: { cwd?: string; timeoutMs?: number },
): CommandResult => {
  const result = spawnSync(command, args, {
    cwd: options?.cwd,
    stdio: 'inherit',
    timeout: options?.timeoutMs,
  });

  const timedOut = (result.error as { code?: string } | undefined)?.code === 'ETIMEDOUT';

  return {
    ok: !result.error && result.status === 0,
    code: result.status,
    stdout: '',
    stderr: result.error ? String(result.error.message) : '',
    timedOut,
  };
};

export const isCommandAvailable = (command: string): boolean => {
  const result = spawnSync(command, ['--help'], {
    stdio: 'ignore',
    timeout: 2_000,
  });
  return !result.error;
};
