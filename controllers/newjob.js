const redisModel = require('../models/redis')

module.exports = function (app) {
	const getNewJobModel = function () {
		return redisModel.getStatusCounts()
			.then(countObject => ({
				counts: countObject,
				newjob: true,
				type: 'New Job'
			}))
	}

	app.get('/newjob', function (req, res) {
		return getNewJobModel(req, res)
			.then(model => res.render('newJob', model))
	})

	app.get('/api/newjob', function (req, res) {
		return getNewJobModel(req, res)
			.then(model => res.json(model))
	})
}
