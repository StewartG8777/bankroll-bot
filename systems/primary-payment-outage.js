'use strict';
// Conservative degraded-mode gates. Financial operations remain unavailable
// until independent provider reconciliation succeeds. No silent override.
const PAYMENT_PATHS=new Set(['/webhook','/monzo-webhook']);
function paymentOutageGate({degraded=false}={}){
 return (req,res,next)=>{
  if(!degraded||!PAYMENT_PATHS.has(req.path)||req.method!=='POST')return next();
  res.setHeader('Retry-After','300');
  return res.status(503).send('Payment verification unavailable; retry later.');
 };
}
function isReadOnlyInteraction(interaction){
 // Explicitly limited to known harmless informational commands.
 return interaction?.isChatInputCommand?.()===true &&
  new Set(['help','ping']).has(interaction.commandName);
}
module.exports={paymentOutageGate,isReadOnlyInteraction};
