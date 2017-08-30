//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license.
//
// Microsoft Bot Framework: http://botframework.com
//
// Bot Framework Emulator Github:
// https://github.com/Microsoft/BotFramwork-Emulator
//
// Copyright (c) Microsoft Corporation
// All rights reserved.
//
// MIT License:
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED ""AS IS"", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//


import { getSettings, dispatch } from './settings';
import * as Settings from './settings';
import * as url from 'url';
import * as path from 'path';
import * as log from './log';
import { Emulator, emulator } from './emulator';
var pjson = require('../../package.json');

process.on('uncaughtException', (error: Error) => {
    console.error(error);
    log.error('[err-server]', error.message.toString(), JSON.stringify(error.stack));
});


Emulator.startup();
setTimeout(function(){
    Settings.getStore().dispatch({
        type: 'Bots_AddOrUpdateBot',
        state: {
            bot: {
                botId : 'g33398m2g97e03ni9',
                botUrl: 'http://localhost:9090/api/messages'
            }
        }
    });

    Settings.getStore().dispatch({
        type: 'ActiveBot_Set',
        state: { 
            botId : 'g33398m2g97e03ni9'                
        }
    });

    Settings.getStore().dispatch({
        type:'Framework_botStorage_Set',
        state:{
            botStoragePath: "mongodb://localhost:27017/botStorage"
        }
    });
}, 1000);