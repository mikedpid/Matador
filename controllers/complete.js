const redisModel = require('../models/redis')

module.exports = function (app) {
	const requestComplete = function () {
		return redisModel.getStatus('complete')
			.then(completed => redisModel.getJobsInList(completed))
			.then(keys => redisModel.formatKeys(keys))
			.then(keyList => Promise.all([keyList, redisModel.getStatusCounts()]))
			.then(([keyList, countObject]) => {
				return {
					keys: keyList,
					counts: countObject,
					complete: true,
					type: 'Complete'
				}
			})
	}

	app.get('/complete', function (req, res) {
		return requestComplete(req, res)
			.then(model => res.render('jobList', model))
	})

	app.get('/api/complete', function (req, res) {
		return requestComplete(req, res)
			.then(model => res.json(model))
	})
}
