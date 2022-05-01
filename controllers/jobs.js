const bullModel = require('../models/bull')
const redisModel = require('../models/redis')

module.exports = function (app) {
	app.get('/api/jobs/pending/status/:type', function (req, res) {
		const type = req.params['type']
		return redisModel.makePendingByType(type)
			.then(results => res.json(results))
	})

	app.get('/api/jobs/pending/id/:type/:id', function (req, res) {
		const id = req.params['id']
		const type = req.params['type']
		return redisModel.makePendingById(type, id)
			.then(results => res.json(results))
	})

	app.get('/api/jobs/delete/status/:type', function (req, res) {
		const type = req.params['type']
		const queueName = req.params['queueName'] ? req.params['queueName'] : null
		return redisModel.deleteJobByStatus(type, queueName)
			.then(results => res.json(results))
	})

	app.get('/api/jobs/delete/id/:type/:id', function (req, res) {
		const id = req.params['id']
		const type = req.params['type']
		return redisModel.deleteJobById(type, id)
			.then(results => res.json(results))
	})

	app.get('/api/jobs/info/:type/:id', function(req, res) {
		const id = req.params['id']
		const type = req.params['type']
		return redisModel.getDataById(type, id)
			.then(results => res.json(results) )
	})

	app.post('/api/jobs/create', function(req, res) {
		let error
		let payload
		const queue = req.body && req.body.queue
		if (!queue) error = 'No queue specified'
		if (!error){
			try {
				payload = JSON.parse(req.body.payload)
			} catch (e) {
				error = 'Invalid JSON'
			}
		}

		if (error) return res.status(400).send(error)
		return bullModel.createJob(req.app.locals.options.redis, queue, payload)
			.then(() => res.status(200).send('OK'))
	})
}
