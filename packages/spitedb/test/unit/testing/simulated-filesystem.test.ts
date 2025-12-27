import { describe, test, expect, beforeEach } from 'bun:test';
import { SimulatedFileSystem } from '../../../src/testing/simulated-filesystem';
import { SimulatedClock } from '../../../src/testing/simulated-clock';

describe('SimulatedFileSystem', () => {
  let fs: SimulatedFileSystem;
  let clock: SimulatedClock;

  beforeEach(() => {
    clock = new SimulatedClock();
    fs = new SimulatedFileSystem(clock);
  });

  describe('basic file operations', () => {
    test('should create and write a file', async () => {
      const handle = await fs.open('/test.txt', 'write');
      const data = new TextEncoder().encode('hello world');

      await fs.write(handle, data);
      await fs.sync(handle);
      await fs.close(handle);

      expect(fs.getFileContentAsString('/test.txt')).toBe('hello world');
    });

    test('should read a file', async () => {
      // Setup
      fs.setFileContent('/test.txt', new TextEncoder().encode('hello'));

      const handle = await fs.open('/test.txt', 'read');
      const content = await fs.read(handle, 0, 100);
      await fs.close(handle);

      expect(new TextDecoder().decode(content)).toBe('hello');
    });

    test('should append to a file', async () => {
      // Create initial content
      fs.setFileContent('/test.txt', new TextEncoder().encode('hello'));

      const handle = await fs.open('/test.txt', 'append');
      await fs.write(handle, new TextEncoder().encode(' world'));
      await fs.sync(handle);
      await fs.close(handle);

      expect(fs.getFileContentAsString('/test.txt')).toBe('hello world');
    });

    test('should throw when reading non-existent file', async () => {
      await expect(fs.open('/nonexistent.txt', 'read')).rejects.toThrow(
        'ENOENT: no such file'
      );
    });

    test('should write at specific offset', async () => {
      fs.setFileContent('/test.txt', new TextEncoder().encode('hello world'));

      const handle = await fs.open('/test.txt', 'readwrite');
      await fs.write(handle, new TextEncoder().encode('XXXXX'), 0);
      await fs.sync(handle);
      await fs.close(handle);

      expect(fs.getFileContentAsString('/test.txt')).toBe('XXXXX world');
    });
  });

  describe('durability semantics', () => {
    test('should not persist writes until sync', async () => {
      const handle = await fs.open('/test.txt', 'write');
      await fs.write(handle, new TextEncoder().encode('unflushed'));

      // Before sync, file should be empty
      expect(fs.getFileContent('/test.txt')?.length).toBe(0);

      await fs.sync(handle);

      // After sync, data should be persisted
      expect(fs.getFileContentAsString('/test.txt')).toBe('unflushed');

      await fs.close(handle);
    });

    test('should lose unflushed writes on crash', async () => {
      const handle = await fs.open('/test.txt', 'write');
      await fs.write(handle, new TextEncoder().encode('will be lost'));

      fs.crash();

      // Data should be lost
      expect(fs.getFileContent('/test.txt')?.length).toBe(0);
    });

    test('should preserve synced data on crash', async () => {
      const handle = await fs.open('/test.txt', 'write');
      await fs.write(handle, new TextEncoder().encode('safe'));
      await fs.sync(handle);
      await fs.close(handle);

      const handle2 = await fs.open('/test.txt', 'append');
      await fs.write(handle2, new TextEncoder().encode(' unsafe'));

      fs.crash();

      // Only synced data should survive
      expect(fs.getFileContentAsString('/test.txt')).toBe('safe');
    });
  });

  describe('fault injection', () => {
    test('should simulate sync failure', async () => {
      fs.injectFault({ syncFails: true });

      const handle = await fs.open('/test.txt', 'write');
      await fs.write(handle, new TextEncoder().encode('data'));

      await expect(fs.sync(handle)).rejects.toThrow('Simulated sync failure');
    });

    test('should simulate read failure', async () => {
      fs.setFileContent('/test.txt', new TextEncoder().encode('data'));
      fs.injectFault({ readFails: true });

      const handle = await fs.open('/test.txt', 'read');

      await expect(fs.read(handle, 0, 100)).rejects.toThrow(
        'Simulated read failure'
      );
    });

    test('should simulate write failure', async () => {
      fs.injectFault({ writeFails: true });

      const handle = await fs.open('/test.txt', 'write');

      await expect(
        fs.write(handle, new TextEncoder().encode('data'))
      ).rejects.toThrow('Simulated write failure');
    });

    test('should simulate partial write', async () => {
      fs.injectFault({ partialWrite: true });

      const handle = await fs.open('/test.txt', 'write');
      await fs.write(handle, new TextEncoder().encode('full data'));
      await fs.sync(handle);
      await fs.close(handle);

      // Only partial data should be written
      const content = fs.getFileContentAsString('/test.txt');
      expect(content?.length).toBeLessThan('full data'.length);
    });

    test('should simulate sync delay', async () => {
      fs.injectFault({ syncDelayMs: 100 });

      const handle = await fs.open('/test.txt', 'write');
      await fs.write(handle, new TextEncoder().encode('data'));

      const startTime = clock.now();
      const syncPromise = fs.sync(handle);

      // Advance clock to complete the sync
      clock.tick(100);
      await syncPromise;

      expect(clock.now() - startTime).toBe(100);
    });

    test('should clear faults', async () => {
      fs.injectFault({ syncFails: true });
      fs.clearFaults();

      const handle = await fs.open('/test.txt', 'write');
      await fs.write(handle, new TextEncoder().encode('data'));
      await fs.sync(handle); // Should not throw

      expect(fs.getFileContentAsString('/test.txt')).toBe('data');
    });
  });

  describe('directory operations', () => {
    test('should create directory', async () => {
      await fs.mkdir('/data');
      expect(await fs.exists('/data')).toBe(true);
    });

    test('should create directory recursively', async () => {
      await fs.mkdir('/a/b/c', { recursive: true });
      expect(await fs.exists('/a')).toBe(true);
      expect(await fs.exists('/a/b')).toBe(true);
      expect(await fs.exists('/a/b/c')).toBe(true);
    });

    test('should list directory contents', async () => {
      await fs.mkdir('/data');
      fs.setFileContent('/data/file1.txt', new Uint8Array(0));
      fs.setFileContent('/data/file2.txt', new Uint8Array(0));

      const entries = await fs.readdir('/data');
      expect(entries).toContain('file1.txt');
      expect(entries).toContain('file2.txt');
    });

    test('should remove empty directory', async () => {
      await fs.mkdir('/empty');
      await fs.rmdir('/empty');
      expect(await fs.exists('/empty')).toBe(false);
    });

    test('should remove directory recursively', async () => {
      await fs.mkdir('/data');
      fs.setFileContent('/data/file.txt', new TextEncoder().encode('data'));

      await fs.rmdir('/data', { recursive: true });

      expect(await fs.exists('/data')).toBe(false);
      expect(await fs.exists('/data/file.txt')).toBe(false);
    });
  });

  describe('file management', () => {
    test('should rename file', async () => {
      fs.setFileContent('/old.txt', new TextEncoder().encode('content'));

      await fs.rename('/old.txt', '/new.txt');

      expect(await fs.exists('/old.txt')).toBe(false);
      expect(fs.getFileContentAsString('/new.txt')).toBe('content');
    });

    test('should delete file', async () => {
      fs.setFileContent('/test.txt', new TextEncoder().encode('data'));

      await fs.unlink('/test.txt');

      expect(await fs.exists('/test.txt')).toBe(false);
    });

    test('should truncate file', async () => {
      fs.setFileContent('/test.txt', new TextEncoder().encode('hello world'));

      const handle = await fs.open('/test.txt', 'readwrite');
      await fs.truncate(handle, 5);
      await fs.close(handle);

      expect(fs.getFileContentAsString('/test.txt')).toBe('hello');
    });

    test('should get file stats', async () => {
      fs.setFileContent('/test.txt', new TextEncoder().encode('hello'));

      const stat = await fs.stat('/test.txt');

      expect(stat.isFile).toBe(true);
      expect(stat.isDirectory).toBe(false);
      expect(stat.size).toBe(5);
    });
  });
});
