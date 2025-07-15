import os, sys, time, aiorun, asyncio, click, datetime, itertools
from concurrent.futures import ThreadPoolExecutor as Executor
import polars as pl
import sqlalchemy as sa

import dal, config, helpers

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

#@click.command()
#@click.argument("path")
def amain(path):
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


# Main CLI
# ========

@click.group()
def cli():
    """fileproc `process run pgm` will run `pgm` against all files in the database that has not been processed yet.
    """
    pass

@click.command()
@click.argument("pgm")
def run(pgm):
    click.echo(f"Running {pgm}")
    ses = dal.get_session()
    result = ses.query(dal.Program).filter(dal.Program.id==pgm)
    print(f"rowcount:{result.count()}")
    for row in result:
       print (row)

@click.command()
@click.argument("pgm")
def add(pgm):
    click.echo(f"Adding {pgm}")
    res = dal.add([dal.Program(id=pgm)])
    print(res)
    
@click.command()
def ls():
    click.echo(f"Listing programs")
    ses = dal.get_session()
    result = ses.query(dal.Program)
    print(f"rowcount:{result.count()}")
    print("--- Programs ---")
    for row in result:
       print(row.id)


@click.command()
@click.argument("pgm")
def rm(pgm):
    click.echo(f"Removing {pgm}")
    ses = dal.get_session()
    result = ses.query(dal.Program).filter(dal.Program.id==pgm)
    print(f"rowcount:{result.count()}")
    if result.count()>0:
        ses.delete(result.first())
        ses.commit()

@click.command()
@click.argument("pgm")
def status(pgm):
    click.echo(f"Checking status for {pgm}")
    ses = dal.get_session()

    res = ses.query(dal.Status).filter(dal.Status.program==pgm)
    print("\n--- Processed files ---")
    print(f"rowcount:{res.count()}")
    for row in res:
       print(row.id)


    print("\n--- Unprocessed files ---")    
    stmt = sa.select(dal.File.__table__.c.id, dal.File.__table__.c.path, dal.File.__table__.c.folder).where(
        sa.not_(
            dal.File.__table__.c.id.in_(
                sa.select(dal.Status.__table__.c.file).where(dal.Status.__table__.c.program==pgm)
            )
        )
    ).subquery()

    with dal.get_engine().connect() as con:
        res = con.execute(sa.select(stmt.c.folder, sa.func.count(stmt.c.id)).group_by(stmt.c.folder))
        print(f"rowcount:{res.rowcount}")
        for row in res:
           print(f"folder:{row[0]}, count:{row[1]}")
    

@click.command()
def stats():
    click.echo(f"Stats")

    print("\n--- Stats ---")    
    stmt = sa.select(dal.File.__table__.c.folder, sa.func.count(dal.File.__table__.c.id)).group_by(
        dal.File.__table__.c.folder
    ).order_by(dal.File.__table__.c.folder)

    with dal.get_engine().connect() as con:
        res = con.execute(stmt)
        print(f"rowcount:{res.rowcount}")
        for row in res:
           print(f"folder:{row[0]}, count:{row[1]}")
    

cli.add_command(run)
cli.add_command(add)
cli.add_command(ls)
cli.add_command(rm)
cli.add_command(status)
cli.add_command(stats)


if __name__== '__main__':
    cli()
    #amain()
