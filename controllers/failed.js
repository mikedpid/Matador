const redisModel = require('../models/redis')

module.exports = function (app) {
	const getFailedData = function () {
		return redisModel.getStatus('failed')
			.then(failed => redisModel.getJobsInList(failed))
			.then(keys => redisModel.formatKeys(keys))
			.then(keyList => Promise.all([keyList, redisModel.getStatusCounts()]))
			.then(([keyList, countObject]) => {
				return {
					keys: keyList,
					counts: countObject,
					failed: true,
					type: 'Failed'
				}
			})
	}

	app.get('/failed', function (req, res) {
		return getFailedData()
			.then(model => res.render('jobList', model))
	})

	app.get('/api/failed', function (req, res) {
		return getFailedData()
			.then(model => res.json(model))
	})
}
