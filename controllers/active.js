const redisModel = require('../models/redis')

module.exports = function (app) {
	const requestActive = function () {
		return redisModel.getStatus('active')
			.then(active => redisModel.getJobsInList(active))
			.then(keys => redisModel.formatKeys(keys))
			.then(formattedKeys => redisModel.getProgressForKeys(formattedKeys))
			.then(keyList => Promise.all([keyList, redisModel.getStatusCounts()]))
			.then(([keyList, countObject]) => {
				return {
					keys: keyList,
					counts: countObject,
					active: true,
					type: 'Active'
				}
			})
	}

	app.get('/active', function (req, res) {
		return requestActive()
			.then(model => res.render('jobList', model))
	})

	app.get('/api/active', function (req, res) {
		return requestActive()
			.then(model => res.json(model))
	})
}
