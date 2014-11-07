$        = require "bling"
log      = $.logger "[rabbit]"
RabbitJS = require 'rabbit.js'

class Rabbit extends $.Promise
	constructor: -> super @

	connect: (url) ->
		if @resolved or @rejected then @reconnect(url)
		log "Connecting to...", url
		context = RabbitJS.createContext url
		# note-taking for multiple subscriptions to the same channel
		context._sockets = Object.create null
		context._patterns = Object.create null
		context.on "ready", =>
			log "Connected."
			@resolve context
		context.on "error", (err) =>
			log "Failed to connect to",url,err
			@reject err
		@

	reconnect: (url) ->
		unless @resolved or @rejected then @connect(url)
		else @wait (err, context) =>
			@reset()
			log "Reconnecting...", url
			if context
				for chan, list of context._patterns # unpack existing subscriptions
					for args in list # queue them up to be resubscribed once the new connection is ready
						log "Re-subscribing...", chan, args.p
						@subscribe chan, args.p, args.h
			@connect(url)

	publish:   (chan, msg) ->
		if arguments.length < 2 or (not $.is 'string', chan)
			throw new Error("Invalid arguments to publish 0: #{String chan} 1: #{String msg}")
		try return p = $.Promise()
		finally @then (context) ->
			pub = context.socket 'PUB'
			pub.connect chan, ->
				pub.write JSON.stringify(msg), 'utf8'
				pub.close()
				p.resolve()

	subscribe: (c, p, h) ->
		if $.is 'function', c      # support (func) arguments as (default, null, func)
			[h, p, c] = [c, null, rabbitChannel()]
		else if $.is 'function', p # support (chan, func) arguments as (chan, null, func)
			[h, p] = [p, null]
		# otherwise, assume arguments as (chan, pattern, func)
		p ?= $.matches.Any
		@then (context) ->
			if err? then return log "error:", err
			sub = context._sockets[c]
			args = { p, h }
			onData = null
			if sub? # we already have subscriptions to this channel
				context._patterns[c].push args
				log "subscribed to channel", c, p
			else
				context._patterns[c] = [ args ]
				context._sockets[c] = sub = context.socket 'SUB'
				sub.on 'data', onData = (data) ->
					try data = JSON.parse data
					catch err then return log "JSON.parse error:", err.message, "in", data
					for args in context._patterns[c] when $.matches args.p, data
						try args.h data
						catch err then log "error in handler:", err.stack
				sub.on 'drain', onData
				sub.connect c, ->
					log "subscribed to channel", c, p

$.extend module.exports, new Rabbit()

if require.main is module
	rabbit = new Rabbit()
	url = $.config.get("AMQP_URL")
	unless url?
		console.log "Must set AMQP_URL in the environment: env AMQP_URL=amqp://... #{process.argv.join ' '}"
		console.log process.env
		process.exit 1
	chan = $.config.get("AMQP_CHANNEL", "test")
	rabbit.connect(url).then ->
		rabbit.subscribe chan, (m) ->
			console.log "(#{$.now - m.ts}ms)->:", m
		$.delay 100, ->
			log "publishing..."
			rabbit.publish(chan, { op: "ping", ts: $.now }).then ->
				$.delay 100, ->
					rabbit.reconnect(url).then ->
						rabbit.publish(chan, { op: "ping", ts: $.now }).then ->
							$.delay 100, ->
								process.exit 0
