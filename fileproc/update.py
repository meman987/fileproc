import os, sys, time, aiorun, asyncio, click, datetime, itertools
from concurrent.futures import ThreadPoolExecutor as Executor
import polars as pl

import dal, helpers

class aScan():
    cnt = 0
    total = 0
    
    def status(self):
        if aScan.cnt % 100 == 0:
            helpers.log(f"aScan count: {aScan.cnt} of {aScan.total} finished")
    
    def myscan(self, path):
        res = []
        for f in os.scandir(path): 
            if f.is_file():
                row = tuple(map(datetime.datetime.fromtimestamp, list(f.stat())[7:]))
                row = (f.name, path.path.split('/')[-1], f.stat()[6]) + row
                res.append(row)

        aScan.cnt += 1
        self.status()
        return res

    async def ascan_folders(self, path, loop_):
        folders, folders2 = itertools.tee(os.scandir(path))
        aScan.total = sum(1 for f in folders2 if f.is_dir())

        futures = [ loop_.run_in_executor(None, self.myscan, p) for p in folders if p.is_dir()]
        res = await asyncio.gather(*futures)
        res = [row for lst in res for row in lst ]
        res =  pl.DataFrame(res, orient='row', schema=dal.COLUMNS)
        
        helpers.log("Starting to write folders to database....")
        res = await dal.write_new(res)
        helpers.log("Finished writing folders to database!")
        return res

async def main():
    print(f'{time.ctime()} Start')
    await asyncio.sleep(1.0)
    print(f'{time.ctime()} Exit program with ctrl-C (when program has finished)!')

def blocking():
    time.sleep(2.0)
    res = f'{time.ctime()} Hello from a thread'
    return res

async def blocking_wrapper(loop_):
    future = loop_.run_in_executor(None, blocking)
    res = await asyncio.gather(future)
    print(res)

@click.command()
@click.argument("path")
def amain(path):
    """fileproc `update` will check all folders in <path> for files and add these to the database.
       Path should point to a structure with files. Is assumed to be on the format: <path>/<folder>/<file>.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    executor = Executor()
    loop.set_default_executor(executor)
    
    loop.create_task(main())
    loop.create_task(blocking_wrapper(loop))
    
    scan_task = loop.create_task(aScan().ascan_folders(path, loop))
    res = loop.run_until_complete(scan_task)
    
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print('Cancelled')
    tasks = asyncio.all_tasks(loop=loop)
    for t in tasks:
        t.cancel()
    group = asyncio.gather(*tasks, return_exceptions=True)
    loop.run_until_complete(group)
    executor.shutdown(wait=True)
    loop.close()
    
if __name__== '__main__':
    amain()
