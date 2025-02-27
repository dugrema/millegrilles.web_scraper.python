import asyncio
import logging
from asyncio import TaskGroup
from collections.abc import Awaitable
from concurrent.futures.thread import ThreadPoolExecutor

from millegrilles_messages.bus.BusContext import StopListener, ForceTerminateExecution
from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector

from millegrilles_webscraper.Configuration import WebScraperConfiguration
from millegrilles_webscraper.Context import WebScraperContext

LOGGER = logging.getLogger(__name__)


async def force_terminate_task_group():
    """Used to force termination of a task group."""
    raise ForceTerminateExecution()


async def main():
    config = WebScraperConfiguration.load()
    context = WebScraperContext(config)

    LOGGER.setLevel(logging.INFO)
    LOGGER.info("Starting")

    # Wire classes together, gets awaitables to run
    coros = await wiring(context)

    try:
        # Use taskgroup to run all threads
        async with TaskGroup() as group:
            for coro in coros:
                group.create_task(coro)

            # Create a listener that fires a task to cancel all other tasks
            async def stop_group():
                group.create_task(force_terminate_task_group())
            stop_listener = StopListener(stop_group)
            context.register_stop_listener(stop_listener)

    except* (ForceTerminateExecution, asyncio.CancelledError):
        pass  # Result of the termination task


async def wiring(context: WebScraperContext) -> list[Awaitable]:
    # Some threads get used to handle sync events for the duration of the execution. Ensure there are enough.
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=10))

    # Create instances
    bus_connector = MilleGrillesPikaConnector(context)
    context.bus_connector = bus_connector
    # filehost_manager = FilehostManager(context)
    # backup_transfer = FilehostBackupTransfer(context, filehost_manager)
    # primary_loader = FilehostPrimaryLoader(context, filehost_manager, backup_transfer)
    #
    # # Add listeners on the manager's primary controler event
    # filehost_manager.add_primary_event_listener(primary_loader.toggle_primary)
    # filehost_manager.add_primary_event_listener(backup_transfer.toggle_primary)
    #
    # command_handler = CommandHandler(context, filehost_manager, primary_loader)
    # await command_handler.setup()

    # Create tasks
    coros = [
        context.run(),
        bus_connector.run(),
        # filehost_manager.run(),
        # backup_transfer.run(),
        # primary_loader.run(),
    ]

    return coros


if __name__ == '__main__':
    asyncio.run(main())
    LOGGER.info("Stopped")
