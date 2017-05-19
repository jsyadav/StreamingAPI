(function($)
		{
	$(document).ready(function()
			{
		//console.log('start');
		// Check if there was a saved application state
		var stateCookie = org.cometd.COOKIE?org.cometd.COOKIE.get('org.cometd.demo.state'):null;
		var state = stateCookie ? org.cometd.JSON.fromJSON(stateCookie) : null;
		var chat = new Chat(state);

		// restore some values
		if (state)
		{
			$('#result').val(state.username);
			$('#useServer').attr('checked',state.useServer);
		}

		// Setup UI
		$('#join').show();        
		$('#sendStopButton').click(function() { chat.join(); });       
		$('#result').attr('autocomplete', 'off').focus();        
		$('#phrase').attr('autocomplete', 'off');
		$('#phrase').keyup(function(e)
				{
			if (e.keyCode == 13)
			{
				chat.send();
			}
				});
			});

	function Chat(state)
	{
		var _self = this;
		var _wasConnected = false;
		var _connected = false;
		var _phrase;
		var _disconnecting = false;
		var _chatSubscription;
		var _channel = '/itb/demo';
		var _sent = false;


		this.join = function()
		{
			//console.log("state " + _connected);
			if (_sent)
			{
				_self.leave()
				_sent = false;
				$('#sendStopButton').html('Send');  
			}else{
				_self.send()
				_sent = true;
				$('#sendStopButton').html('Stop');
			}

		};

		this.leave = function()
		{
			if (_chatSubscription)
			{
				$.cometd.unsubscribe(_chatSubscription);
				_chatSubscription = null;
			}
			//$.cometd.disconnect();

			$('#result').focus();

			_disconnecting = true;
		};

		this.send = function()
		{
			var phrase = $('#phrase');
			var text = phrase.val();
			var _delay = $('#delay').val();
            var _limit = $('#limit').val();
            var _type = $('#type').val();
            var _validity = $('#validity').val();
            var _key = 'developerKey';
            
			if (!text || !text.length) 
			{
				alert("Search String can't be empty..");
				return;
			}

			if (!_chatSubscription)
			{
				_chatSubscription = $.cometd.subscribe(_channel, _self.receive);
			}
			//console.log("Sending " + text);
			$.cometd.publish("/service/itb", {
				room: _channel,
				validity: _validity,
				user:_key,
				limit: _limit,
                delay: _delay,
                type : _type,
				phrase: text             
			});

		};

		this.receive = function(message)
		{
			//console.log("in receive");

			var id = message.id;
			var posts = new Array();

			posts = message.data;
			//console.log(posts);

			var length = posts.length;
			//console.log(length);

			if (length <=0){
                alert("Search returned 0 result, change the validity or query string");
                return;
			
			}
			for (i=0;i<length;i++){
				//console.log(posts[i]);
				//_appendResults('', posts[i].img_url, posts[i].tweet_url, posts[i].tweet_data);
				_appendResults('', posts[i].img_url, posts[i].tweet_url, posts[i].tweet_data, posts[i].Sentiment_Analysis,
						posts[i].UserProfile, posts[i].activity_analysis, posts[i].gender, posts[i].ner);
			}

		};

		function _appendResults(id, i_url, t_url, t_data, sentimentAnalysis,userProfile, activityAnalysis, gender, ner)
		{
			/*
			var jsonSA = xml2json.parser(sentimentAnalysis, "","compact");
            var jsonAA = xml2json.parser(activityAnalysis, "","compact");
            var jsonUP = xml2json.parser(userProfile, "","compact");
            var jsonGEN = xml2json.parser(gender, "","compact");
            var jsonNER = xml2json.parser(ner, "","compact");
            */
			//console.log("in _appendResults");

			//console.log(i_url);

			//$('#result').prepend('<div class="result" id="'+id+'"><a target="_blank" href="http://twitter.com/'+user+'/statuses/'+id+'"><img param="http://twitter.com/'+user+'/statuses/'+id+'" src="'+avatar+'" /></a><p>'+new_text+'</p></div>');
			//$('#result').prepend ('<div class="result">'+'<a target="_blank" href="'+t_url+'"><img src="'+i_url+'"/></a><p>'+t_data+'</p></div>');
			//$('#result').prepend ('<div class="result">'+'<a target="_blank" href="'+t_url+'"><img src="'+i_url+'"/></a>&nbsp;&nbsp;'+t_data+'<br>&lt;activity_likelhood&gt;'+likelihood+'&lt;/activity_likelihood&gt;</div>');
			//	$('#result').prepend ('<div class="result">'+'<a target="_blank" href="'+t_url+'"><img src="'+i_url+'"/></a>&nbsp;&nbsp;'+t_data+'<br><textarea rows="20" cols="100" style="border:none;">'+sentimentAnalysis+'\n'+userProfile+'</textarea></div>');
			//$('#result').prepend ('<div class="result">'+'<a target="_blank" href="'+t_url+'"><img src="'+i_url+'"/></a>&nbsp;&nbsp;'+t_data+'<br><textarea rows="20" cols="100" style="border:none;">'+sentimentAnalysis+'\n\n'+userProfile+'\n\n'+activityAnalysis+'\n\n'+gender+'\n\n'+ner+'</textarea></div>');
            $('#result').prepend ('<div class="result">'+'<a target="_blank" href="'+t_url+'"><img src="'+i_url+'"/></a>&nbsp;&nbsp;'+t_data+'<br><textarea rows="20" cols="100" style="border:none;">'+jsonSA+'\n\n'+jsonUP+'\n\n'+jsonAA+'\n\n'+jsonGEN+'\n\n'+jsonNER+'\n\n</textarea></div>');

		}

		function _metaChannelHandler(message){
			console.log("Message is "+ message.channel);
			if (message.channel == "/meta/connect" && message.successful )
			{
				//console.log("/meta/connect " + message.successful);      

				if (_disconnecting)
				{
					//return;
				}
				_wasConnected = _connected;
				_connected = message.successful === true;
				if (!_wasConnected && _connected)
				{
					// Reconnect
					//_chatSubscription = $.cometd.subscribe(_channel, _self.receive);
				}
				else if (_wasConnected  && !_connected)
				{
					// Disconnected
				}
			}
			if (message.channel == "/meta/disconnect" && message.successful)
			{
				_connected = false;
				//console.log("/meta/disconnect " + message.successful);
			}

			if (message.channel == "/meta/handshake" && message.successful)
			{
				//console.log("/meta/handshake " + message.successful);      
			}

		}

		{
			// Initialization 
			// Default server
			var cometdURL = location.protocol + "//" + location.host + "/cometd";
			//var cometdURL = location.protocol + "//" + location.host + config.contextPath + "/cometd";

			$.cometd.configure({
				url: cometdURL,
				logLevel: 'info'
			});

			// Do client server handshake
			$.cometd.handshake();

			$('#phrase').focus();

		}

		$.cometd.addListener('/meta/handshake', _metaChannelHandler);       
		$.cometd.addListener('/meta/connect', _metaChannelHandler);
		$.cometd.addListener('/meta/disconnect', _metaChannelHandler);
		$.cometd.addListener('/meta/subscribe', _metaChannelHandler);
		$.cometd.addListener('/meta/publish', _metaChannelHandler);
		$.cometd.addListener('/meta/unsubscribe', _metaChannelHandler);
		$.cometd.addListener('/meta/unsuccessful', _metaChannelHandler);

		// Restore the state, if present
		if (state)
		{
			setTimeout(function()
					{
				// This will perform the handshake
				_self.join(state.username);
					}, 0);
		}

		$(window).unload(function()
				{
			if ($.cometd.reload)
			{
				$.cometd.reload();
				// Save the application state only if the user was chatting
				if (_wasConnected && _phrase)
				{
					var expires = new Date();
					expires.setTime(expires.getTime() + 5 * 1000);
					org.cometd.COOKIE.set('org.cometd.demo.state', org.cometd.JSON.toJSON({
						username: _phrase,
					}), { 'max-age': 5, expires: expires });
				}
			}
			else
			{
				$.cometd.disconnect();
			}
				});
	}

		})(jQuery);
