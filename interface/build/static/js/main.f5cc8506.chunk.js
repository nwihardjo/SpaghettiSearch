(window.webpackJsonp=window.webpackJsonp||[]).push([[0],{20:function(e,t,a){var r="http://0.0.0.0:"+(Object({NODE_ENV:"production",PUBLIC_URL:""}).PORT||8080)+"/";e.exports={address:r}},38:function(e,t,a){e.exports=a(73)},43:function(e,t,a){},45:function(e,t,a){},46:function(e,t,a){},67:function(e,t,a){},68:function(e,t,a){},69:function(e,t,a){},70:function(e,t,a){},73:function(e,t,a){"use strict";a.r(t);var r=a(0),n=a.n(r),s=a(32),l=a.n(s),i=(a(43),a(44),a(10)),c=a(11),u=a(14),o=a(12),h=a(13),m=a(74),d=a(75),p=a(76),f=a(77),E=a(78),g=(a(45),function(e){function t(e){var a;return Object(i.a)(this,t),(a=Object(u.a)(this,Object(o.a)(t).call(this,e))).state={word:"",freq:0},a}return Object(h.a)(t,e),Object(c.a)(t,[{key:"componentDidMount",value:function(e){this.setState({word:this.props.word,freq:this.props.freq})}},{key:"render",value:function(){return n.a.createElement("div",{className:"word"},this.state.word," ",n.a.createElement("span",null,"\u2219")," ",this.state.freq)}}]),t}(r.Component)),b=(a(46),a(20),a(21),function(e){function t(e){var a;return Object(i.a)(this,t),(a=Object(u.a)(this,Object(o.a)(t).call(this,e))).renderParent=function(){return a.state.Parents.length>0?n.a.createElement("div",null,n.a.createElement("b",null," Parents: "),a.state.Parents.map(function(e,t){return n.a.createElement("a",{href:e},"Parent",t+1," ","   ")})):n.a.createElement("div",null)},a.renderChildren=function(){return a.state.Children.length>0?n.a.createElement("div",null,n.a.createElement("b",null," Children: "),a.state.Children.map(function(e,t){return n.a.createElement("a",{href:e},"Child",t+1," ","   ")})):n.a.createElement("div",null)},a.renderSummary=function(){var e=[],t=[];for(var r in a.state.Term){var s=a.state.Summary.toLowerCase().indexOf(a.state.Term[r].toLowerCase());-1!==s&&t.push([r,s])}if(0===t.length)return n.a.createElement("div",null,a.state.Summary);var l=t.sort(function(e,t){return e[1]-t[1]});for(var i in e.push(n.a.createElement("span",null,a.state.Summary.slice(0,l[0][1]))),l)e.push(n.a.createElement("b",null,a.state.Summary.slice(l[i][1],l[i][1]+a.state.Term[l[i][0]].length))),l.length-1===Number(i)?e.push(n.a.createElement("span",null,a.state.Summary.slice(l[i][1]+a.state.Term[l[i][0]].length))):e.push(n.a.createElement("span",null,a.state.Summary.slice(l[i][1]+a.state.Term[l[i][0]].length,l[Number(i)+1][1])));return e},a.state={Url:"",Page_title:"",Mod_date:Date(),Page_size:0,Children:[],Parents:[],Words_mapping:{},PageRank:0,FinalRank:0,Summary:"",Term:[]},a}return Object(h.a)(t,e),Object(c.a)(t,[{key:"componentDidMount",value:function(e){var t=this.props.data.Mod_date.match(/(\d{4})-(\d{2})-(\d{2})/);this.setState({Url:this.props.data.Url,Mod_date:t[0],Page_title:this.props.data.Page_title,Page_size:this.props.data.Page_size,Children:null!=this.props.data.Children?this.props.data.Children:[],Parents:null!=this.props.data.Parents?this.props.data.Parents:[],Words_mapping:null!=this.props.data.Words_mapping?this.props.data.Words_mapping:{},PageRank:this.props.data.PageRank,FinalRank:this.props.data.FinalRank,Summary:this.props.data.Summary,Term:this.props.terms})}},{key:"render",value:function(){return n.a.createElement("a",{className:"card-link--nostyle",href:this.state.Url},n.a.createElement(m.a,{className:"custom"},n.a.createElement(d.a,null,n.a.createElement(p.a,{className:"title",href:this.state.Url}," ",this.state.Page_title," "),n.a.createElement("small",{className:"text-muted"},n.a.createElement("span",null,"\u2219")," ",Math.round(100*this.state.FinalRank)/100,"%"),n.a.createElement(f.a,null,n.a.createElement(p.a,{className:"subtitle",href:this.state.Url}," ",this.state.Url," ")),n.a.createElement("div",{className:"row"},Object.entries(this.state.Words_mapping).sort(function(e,t){return t[1]-e[1]}).map(function(e){return n.a.createElement(g,{word:e[0],freq:e[1]})}))),n.a.createElement(d.a,null,n.a.createElement(E.a,null,this.renderSummary()," ",n.a.createElement("br",null),n.a.createElement("small",{className:"text-muted"},this.renderParent(),this.renderChildren()))),n.a.createElement(d.a,null,n.a.createElement(E.a,null,n.a.createElement("small",{className:"text-muted"},n.a.createElement("b",null,"Modified Date: "),this.state.Mod_date," "," ",n.a.createElement("b",null,"Page Size: "),this.state.Page_size)))))}}]),t}(r.Component)),y=(a(67),a(20)),v=a(21),P=function(e){function t(e){var a;return Object(i.a)(this,t),(a=Object(u.a)(this,Object(o.a)(t).call(this,e))).getResults=function(e){a.setState({results:[],query:e}),v({method:"post",url:y.address+"query",data:{Query:e},headers:{"Content-Type":"application/json"}}).then(function(t){for(var r=t.data.filter(function(e){return""!=e.Url}),n=a.state.query,s=a.state.query.match(/".*?"/g),l=s?s.length:0,i=[],c=0;c<l;c++)n=n.replace(s[c],""),s[c]=s[c].slice(1,-1),0!==s[c].length&&i.push(s[c]);for(var u=n.split(" "),o=0;o<u.length;o++){var h=u[o].trim();0!==h.length&&i.push(h)}console.log(i),a.setState({query:e,results:r,terms:i})}).catch(function(e){})},a.state={query:"",results:[],terms:[]},a}return Object(h.a)(t,e),Object(c.a)(t,[{key:"componentDidMount",value:function(e){this.setState({query:this.props.query}),this.getResults(this.props.query)}},{key:"render",value:function(){var e=this;return n.a.createElement("div",{className:"results"},this.state.results.map(function(t,a){return n.a.createElement(b,{data:t,terms:e.state.terms})},this))}}]),t}(r.Component),C=a(7),k=Object(C.a)(),S=a(79),j=a(80),O=a(81),w=a(82),q=a(83),x=(a(68),function(e){function t(e){var a;return Object(i.a)(this,t),(a=Object(u.a)(this,Object(o.a)(t).call(this,e))).handleChange=function(e){a.setState({query:e.target.value})},a.getResults=function(e){e.preventDefault(),a.resultsElement.current.getResults(a.state.query)},a.resultsElement=n.a.createRef(),"undefined"!==typeof k.location.state?a.state={query:k.location.state.query,results:[]}:a.state={query:e.query,results:[]},a}return Object(h.a)(t,e),Object(c.a)(t,[{key:"componentDidMount",value:function(e){"undefined"!==typeof k.location.state?this.setState({query:k.location.state.query}):this.setState({query:this.props.query})}},{key:"render",value:function(){return n.a.createElement("div",null,n.a.createElement(S.a,{color:"faded",light:!0,className:"header",sticky:"top"},n.a.createElement(j.a,{href:"/"},n.a.createElement("b",null,"Spaghetti")),n.a.createElement(O.a,{className:"mr-auto",navbar:!0},n.a.createElement(w.a,{onSubmit:this.getResults},n.a.createElement(q.a,{id:"searchbar",type:"search",className:"searchbox--results",placeholder:"What would you like to search?",defaultValue:this.state.query,onChange:this.handleChange})))),n.a.createElement(P,{ref:this.resultsElement,query:this.state.query}))}}]),t}(r.Component)),R=(a(69),a(84)),D=function(e){function t(){var e;return Object(i.a)(this,t),(e=Object(u.a)(this,Object(o.a)(t).call(this))).handleSearch=function(t){e.setState({showResults:!0})},e.handleChange=function(t){t.preventDefault(),e.setState({query:t.target.value}),console.log(e.state.query)},e.state={query:"",showResults:!1},e}return Object(h.a)(t,e),Object(c.a)(t,[{key:"componentDidMount",value:function(){}},{key:"render",value:function(){return this.state.showResults?n.a.createElement(x,{query:this.state.query}):n.a.createElement("div",{className:"App"},n.a.createElement("div",null,n.a.createElement("h1",null,"Hello,"),n.a.createElement(w.a,{onSubmit:this.handleSearch},n.a.createElement(q.a,{type:"search",className:"searchbox--main",placeholder:"What would you like to search?",bsSize:"lg",onChange:this.handleChange})),n.a.createElement("br",null),n.a.createElement("small",{className:"text-muted"},"OR TRY ",n.a.createElement("a",{href:"/word-list"},n.a.createElement(R.a,{outline:!0,color:"primary"},"Keyword Search")))))}}]),t}(r.Component),N=a(85),_=a(86),L=a(87),M=a(88),T=a(89),W=a(90),U=(a(70),a(21)),z=a(20),A=function(e){function t(){var e;return Object(i.a)(this,t),(e=Object(u.a)(this,Object(o.a)(t).call(this))).handleSubmit=function(){console.log(e.state.termList.join(" ")),k.push("/query",{query:e.state.termList.join(" ")}),document.location.reload(!0)},e.state={data:"",maxRow:15,maxCol:5,paginationWinSize:5,currPage:1,currPageD:[],currPre:"A",termList:[]},e}return Object(h.a)(t,e),Object(c.a)(t,[{key:"componentDidMount",value:function(e){var t=this;U.get(z.address+"wordlist/a").then(function(e){var a=[];for(var r in e.data){if(r>=t.state.maxRow*t.state.maxCol)break;a.push(e.data[r])}t.setState({data:e.data,currPageD:a})})}},{key:"updateCurrData",value:function(e){var t=this;U.get(z.address+"wordlist/"+e.toLowerCase()).then(function(a){var r=[];for(var n in a.data){if(n>=t.state.maxRow*t.state.maxCol)break;r.push(a.data[n])}t.setState({data:a.data,currPageD:r,currPage:1,currPre:e})})}},{key:"addTermList",value:function(e){this.state.termList.includes(e)?this.setState({termList:this.state.termList.filter(function(t){return t!==e})}):this.setState({termList:this.state.termList.concat([e])})}},{key:"getCurrPageArr",value:function(){for(var e=[],t=0;t<this.state.maxRow;t++){for(var a=[],r=0;r<this.state.maxCol;r++){var s=this.state.currPageD[t*this.state.maxCol+r];a.push(n.a.createElement(N.a,{key:r},n.a.createElement(R.a,{color:"link",onClick:this.addTermList.bind(this,s)},s)))}e.push(n.a.createElement(_.a,{key:t},a))}return e}},{key:"updateCurrPageD",value:function(e){for(var t=[],a=(e-1)*this.state.maxRow*this.state.maxCol,r=0;r<this.state.maxCol*this.state.maxRow&&!(a+r>=this.state.data.length);r++)t.push(this.state.data[a+r]);this.setState({currPageD:t,currPage:e})}},{key:"getPagination",value:function(){var e=[],t=Math.ceil(this.state.data.length/(this.state.maxRow*this.state.maxCol));if(t<=5)for(var a=0;a<t;a++)e.push(n.a.createElement(L.a,{key:a,active:this.state.currPage===a+1},n.a.createElement(M.a,{href:"#",onClick:this.updateCurrPageD.bind(this,a+1)},a+1)));else{var r=this.state.currPage-Math.floor(this.state.paginationWinSize/2),s=this.state.currPage+Math.floor(this.state.paginationWinSize/2);r<1&&(s+=1-r,r=1),s>t&&(r-=s-t,s=t);for(var l=r;l<=s;l++)e.push(n.a.createElement(L.a,{key:l,active:this.state.currPage===l},n.a.createElement(M.a,{href:"#",onClick:this.updateCurrPageD.bind(this,l)},l)))}return n.a.createElement(T.a,{"aria-label":"Page navigation example",style:{justifyContent:"right"}},n.a.createElement(L.a,{disabled:1===this.state.currPage},n.a.createElement(M.a,{first:!0,href:"#",onClick:this.updateCurrPageD.bind(this,1)})),n.a.createElement(L.a,{disabled:1===this.state.currPage},n.a.createElement(M.a,{previous:!0,href:"#",onClick:this.updateCurrPageD.bind(this,this.state.currPage-1)})),e,n.a.createElement(L.a,{disabled:this.state.currPage===t},n.a.createElement(M.a,{next:!0,href:"#",onClick:this.updateCurrPageD.bind(this,this.state.currPage+1)})),n.a.createElement(L.a,{disabled:this.state.currPage===t},n.a.createElement(M.a,{last:!0,href:"#",onClick:this.updateCurrPageD.bind(this,t)})))}},{key:"render",value:function(){var e=["ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"],t=[];for(var a in e[0])t.push(n.a.createElement(R.a,{key:a,block:!0,color:this.state.currPre===e[0][a]?"secondary":"info",disabled:this.state.currPre===e[0][a],onClick:this.updateCurrData.bind(this,e[0][a])},e[0][a]));return n.a.createElement("div",{className:"WordList"},n.a.createElement("br",null),n.a.createElement("br",null),n.a.createElement("div",{className:"alphabet"},t),n.a.createElement("br",null),n.a.createElement("br",null),this.getCurrPageArr(),n.a.createElement("br",null),n.a.createElement("br",null),n.a.createElement(W.a,null,n.a.createElement(_.a,null,n.a.createElement(N.a,null,"Selected Terms: ",n.a.createElement("br",null),"["+this.state.termList.join(", ")+"]"),n.a.createElement(N.a,null,n.a.createElement(R.a,{color:"primary",onClick:this.handleSubmit,block:!0},"Search")),n.a.createElement(N.a,null,this.getPagination())),n.a.createElement("br",null)))}}]),t}(r.Component),F=a(35),B=a(16);var I=function(){return n.a.createElement(F.a,{history:k},n.a.createElement(B.c,null,n.a.createElement(B.a,{path:"/",exact:!0,component:Object(B.f)(D)}),n.a.createElement(B.a,{path:"/query",exact:!0,component:Object(B.f)(x)}),n.a.createElement(B.a,{path:"/word-list",component:Object(B.f)(A)})))};Boolean("localhost"===window.location.hostname||"[::1]"===window.location.hostname||window.location.hostname.match(/^127(?:\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}$/));l.a.render(n.a.createElement(I,null),document.getElementById("root")),"serviceWorker"in navigator&&navigator.serviceWorker.ready.then(function(e){e.unregister()})}},[[38,1,2]]]);
//# sourceMappingURL=main.f5cc8506.chunk.js.map