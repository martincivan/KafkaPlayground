import os
from unittest import IsolatedAsyncioTestCase

from consumer.configuration import PathHandlersLoader, HandlerParams


class HandlersLoadersTest(IsolatedAsyncioTestCase):

    async def test_load_from_file(self):
        absolute_path = os.path.dirname(__file__)
        relative_path = "resources/handlers/handlers.json"
        loader = PathHandlersLoader(os.path.join(absolute_path, relative_path))
        real = await loader.load()
        expected = [
            HandlerParams("filevirus-scanner", {'file'}, {'File\\Domain\\Model\\Event\\FileUploaded'}),
            HandlerParams("work-report-legacywork-report-legacy", {'user-slot-report'},
                          {'WorkReport\\Legacy\\Application\\UserSlotReportCommand'}),
            HandlerParams("ticket-searchcommand_handler", {'CommandTopic'},
                          {'TicketSearch\\Application\\IndexTicketsCommand'})

        ]
        self.assertEqual(expected, real)

    async def test_load_from_dir(self):
        absolute_path = os.path.dirname(__file__)
        relative_path = "resources/handlers/"
        loader = PathHandlersLoader(os.path.join(absolute_path, relative_path))
        real = await loader.load()
        expected = [
            HandlerParams(id='filevirus-scanner', topics={'file'}, types={'File\\Domain\\Model\\Event\\FileUploaded'}),
            HandlerParams(id='work-report-legacywork-report-legacy',
                          topics={'some_different_topic', 'user-slot-report', 'both_different'},
                          types={'some_different_event', 'WorkReport\\Legacy\\Application\\UserSlotReportCommand',
                                 'both_different'}),
            HandlerParams(id='ticket-searchcommand_handler', topics={'CommandTopic'},
                          types={'TicketSearch\\Application\\IndexTicketsCommand'}),
            HandlerParams(id='whole_new', topics={'whole_new'}, types={'whole_new'})]
        self.assertEqual(expected, real)
