'use strict';
// Lightweight boundary adapters. They DO NOT change Stripe or Monzo business
// decisions; they prevent an uncommitted local mutation being acknowledged.
const STORAGE_UNAVAILABLE='Google Drive could not confirm this save. No durable success has been confirmed. Please contact the bot owner before retrying.';
function protectInteractionDurability(interaction,manager,logger=console){
  for(const method of ['reply','editReply','followUp','update']){
    if(typeof interaction[method]!=='function')continue;
    const original=interaction[method].bind(interaction);
    interaction[method]=async(...args)=>{
      try{await manager.flush();}
      catch(error){
        logger.error?.('[PrimaryDrive] Blocked unconfirmed Discord acknowledgement:',error.message);
        return original({
          content:STORAGE_UNAVAILABLE,embeds:[],files:[],components:[],
          ...(method==='reply'||method==='followUp'?{ephemeral:true}:{})
        });
      }
      return original(...args);
    };
  }
}
function primaryWebhookGate(manager,logger=console){
  return function primaryWebhookMiddleware(req,res,next){
    if(req.method!=='POST'||!['/webhook','/monzo-webhook'].includes(req.path))return next();
    if(!manager.status().active)return res.status(503).send('Primary storage not ready; retry webhook.');
    const send=res.send.bind(res);
    let pending=false;
    res.send=(...args)=>{
      if(pending||res.statusCode<200||res.statusCode>=300)return send(...args);
      pending=true;
      void manager.flush().then(
        ()=>send(...args),
        error=>{
          logger.error?.('[PrimaryDrive] Refused payment webhook success:',error.message);
          if(res.headersSent){res.destroy(error);return;}
          res.status(503);
          res.removeHeader('Content-Length');res.removeHeader('Content-Type');
          send('Primary storage commit failed; retry webhook.');
        }
      );
      return res;
    };
    next();
  };
}
module.exports={protectInteractionDurability,primaryWebhookGate,STORAGE_UNAVAILABLE};
