const redisModel = require('../models/redis')

module.exports = function (app) {
	const getPendingModel = function() {
		return redisModel.getStatus('wait')
			.then(active => redisModel.getJobsInList(active))
			.then(keys => redisModel.formatKeys(keys))
			.then(keyList => Promise.all([keyList, redisModel.getStatusCounts()]))
			.then(([keyList, countObject]) => {
				return {
					keys: keyList,
					counts: countObject,
					pending: true,
					type: 'Pending'
				}
			})
	}

	app.get('/pending', function (req, res) {
		return getPendingModel()
			.then(model => res.render('jobList', model))
	})

	app.get('/api/pending', function (req, res) {
		return getPendingModel()
			.then(model => res.json(model))
	})
}
