const redisModel = require('../models/redis')

module.exports = function (app) {
	const getQueuesModel = function() {
		return Promise.all([redisModel.getQueues(),redisModel.getStatusCounts()])
			.then(([queues, countObject]) => {
				return {
					keys: queues,
					counts: countObject,
					queues: true,
					type: 'Queues'
				}
			})
	}

	app.get('/queues', function (req, res) {
		return getQueuesModel()
			.then(model => res.render('queueList', model))
	})

	app.get('/api/queues', function (req, res) {
		return getQueuesModel()
			.then(model => res.json(model))
	})
}
