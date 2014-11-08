$        = require "bling"
log      = $.logger "[rabbit]"
RabbitJS = require 'rabbit.js'

class Rabbit extends $.Promise
	constructor: (@verbose = false) ->
		super @

	connect: (url) ->
		if @resolved or @rejected then @reconnect(url)
		if @verbose then log "Connecting to...", url
		context = RabbitJS.createContext url
		# note-taking for multiple subscriptions to the same channel
		context._sockets = Object.create null
		context._patterns = Object.create null
		context.on "ready", =>
			if @verbose then log "Connected."
			@resolve context
		context.on "error", (err) =>
			log "Failed to connect to",url,err
			@reject err
		@

	reconnect: (url) ->
		unless @resolved or @rejected then @connect(url)
		else @wait (err, context) =>
			@reset()
			if @verbose then log "Reconnecting...", url
			if context
				for chan, list of context._patterns # unpack existing subscriptions
					for args in list # queue them up to be resubscribed once the new connection is ready
						if @verbose then log "Re-subscribing...", chan, args.p
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

	subscribe: (c, p, h) -> # (channel, pattern, func)
		if $.is 'function', c      # calling as subcribe(func) is same as (default, null, func)
			[h, p, c] = [c, null, rabbitChannel()]
		else if $.is 'function', p # calling as subscribe(chan, func) is as (chan, null, func)
			[h, p] = [p, null]
		# a null pattern matches Anything
		p ?= $.matches.Any
		@then (context) =>
			if err? then return log "error:", err
			sub = context._sockets[c]
			args = { p, h }
			onData = null
			if sub? # we already have subscriptions to this channel
				context._patterns[c].push args
				if @verbose then log "subscribed to channel", c, p
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
				sub.connect c, =>
					if @verbose then log "subscribed to channel", c, p

	unsubscribe: (c, p, h) ->
		@then (context) ->
			if c? and (not p?) and (not h?)
				context._sockets[c]?.close()
				delete context._sockets[c]
				delete context._patterns[c]
			else if c? and p? and (not h?)
				context._patterns[c] = patterns = $(context._patterns[c]).filter (item) ->
					item.p is p
				if patterns.length is 0 then @unsubscribe(c)
			else if c? and p? and h?
				context._patterns[c] = patterns = $(context._patterns[c]).filter (item) ->
					item.p is p and item.h is h
				if patterns.length is 0 then @unsubscribe(c)

	once: (chan, pattern, handler) ->
		f = =>
			handler.apply arguments[0], arguments
			@unsubscribe chan, pattern, f
		@subscribe chan, pattern, f

$.extend module.exports, new Rabbit()

if require.main is module
	rabbit = new Rabbit true
	url = $.config.get "AMQP_URL"
	unless url?
		console.log "Must set AMQP_URL: > env AMQP_URL=amqp://... #{process.argv[0]} ..."
		process.exit 1
	chan = $.config.get "AMQP_CHANNEL", "test"
	rabbit.connect(url).then ->
		rabbit.subscribe chan, (m) ->
			console.log "(#{$.now - m.ts}ms)->:", m
		$.delay 100, ->
			rabbit.publish(chan, { op: "ping", ts: $.now }).then ->
				$.delay 100, ->
					rabbit.reconnect(url).then ->
						rabbit.publish(chan, { op: "ping", ts: $.now }).then ->
							$.delay 100, ->
								process.exit 0
