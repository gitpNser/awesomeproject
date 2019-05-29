import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web

def index(request):
	return web.Response(body=b'<h1>Awesome<h1>', content_type='text/html')
	
async def init():
	app = web.Application()
	app.router.add_route('GET', '/', index)
	runner = web.AppRunner(app)
	await runner.setup()
	srv = web.TCPSite(runner, '127.0.0.1', 9000)
	await srv.start()
	logging.info('Server started at http://127.0.0.1:9000...')
	return srv
	
loop = asyncio.get_event_loop()
loop.run_until_complete(init())
loop.run_forever()
