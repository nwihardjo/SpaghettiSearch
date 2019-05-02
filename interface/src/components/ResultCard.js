import React, { Component } from 'react';
import {Card, CardText, CardBody, CardSubtitle, CardLink} from 'reactstrap';
import Keywords from './Keywords';
import '../styles/ResultCard.css';
const config = require('../config/server');
const axios = require('axios');

class ResultCard extends Component {
  constructor (props) {
    super(props)
    this.state = {Url: "",
    Page_title: "",
    Mod_date: Date(),
    Page_size: 0,
    Children:[],
    Parents: [],
    Words_mapping:{},
    PageRank: 0,
    FinalRank: 0,
    Summary: "",
		Term: [],}
  }
  componentDidMount (props) {
    // extract only the date
    var date=this.props.data['Mod_date'].match(/(\d{4})-(\d{2})-(\d{2})/)
    this.setState({Url: this.props.data['Url'],
                  Mod_date: date[0],
                  Page_title: this.props.data['Page_title'],
                  Page_size: this.props.data['Page_size'],
                  Children: ((this.props.data['Children']!=null) ? this.props.data['Children']: []),
                  Parents: ((this.props.data['Parents']!=null) ? this.props.data['Parents']: []),
                  Words_mapping: ((this.props.data['Words_mapping']!=null)?this.props.data['Words_mapping']:{}),
                  PageRank: this.props.data['PageRank'],
                  FinalRank: this.props.data['FinalRank'],
                  Summary: this.props.data['Summary'],
									Term: this.props.term});
		console.log(this.state.Term);
  }
  renderParent = () => {
    if(this.state.Parents.length > 0) {
      return(
        <div>
        <b> Parents: </b>
        {
          this.state.Parents.map((link, i) => {
            return(<a href={link}>Parent{i+1} {'   '}</a>)
          })
        }
        </div>
      )
    } else {
      return <div></div>;
    }
  }
  renderChildren = () => {
    if(this.state.Children.length > 0) {
      return(
        <div>
        <b> Children: </b>
        {
          this.state.Children.map((link, i) => {
            return(<a href={link}>Child{i+1} {'   '}</a>)
          })
        }
        </div>
      )
    } else {
      return <div></div>;
    }
  }
	renderSummary = () => {
		var summaryArr = [];
		let idxs = [];
		let x = 0;
		for(let i in this.State.Term) {
			let idx = this.state.Summary.indexOf(i);
			if(idx !== -1) {
				idxs.push([x, idx]);
			}
			x++;
		}
		if(idxs.length === 0) {
			return <div>{this.state.Summary}</div>;
		} else {
			let sortedIdxs = idxs.sort(function(a, b) {
				return a[1] - b[1];
			});
			summaryArr.push(<span>{this.state.Summary.slice(0, sortedIdxs[0][1])}</span>);
			let i = 0;
			for(let x in sortedIdxs) {
						summaryArr.push(<b>{this.state.Summary.slice(x[1], x[1]+this.state.Term[x[0]].length)}</b>);
						if(sortedIdxs.length - 1 === i) {
							summaryArr.push(<span>{this.state.Summary.slice(x[1]+this.state.Term[x[0]].length)}</span>);
						} else {
							summaryArr.push(<span>{this.state.Summary.slice(x[1]+this.state.Term[x[0]].length, sortedIdxs[i+1][1])}</span>);
						}
						i++;
			}
		}
		return summaryArr;
	}
  render() {
    return (
      <a className='card-link--nostyle' href={this.state.Url}>
      <Card className='custom'>
        <CardBody>
          <CardLink className='title' href={this.state.Url}> {this.state.Page_title} </CardLink>
          <small className="text-muted"><span>&#8729;</span> {Math.round(this.state.FinalRank*100)/100}</small>
          <CardSubtitle><CardLink className='subtitle' href={this.state.Url}> {this.state.Url} </CardLink></CardSubtitle>
          <div className='row'>
          {Object.entries(this.state.Words_mapping).sort((a, b) => {
						return b[1] - a[1] }).map((entry) => {
            return(<Keywords word={entry[0]} freq={entry[1]} />)
          })}</div>
        </CardBody>
        <CardBody>
          <CardText>
          {this.renderSummary()} <br/>
          <small className="text-muted">
          {this.renderParent()}
          {this.renderChildren()}
          </small>
          </CardText>
        </CardBody>
        <CardBody>
          <CardText>
          <small className="text-muted">
            <b>Modified Date: </b>{this.state.Mod_date} {' '}
            <b>Page Size: </b>{this.state.Page_size}
          </small>
          </CardText>
        </CardBody>
      </Card>
      </a>
    );
  }
}
// {(this.state.Parents != [])?return(<b> Parents: </b>):}
// {
//   this.state.Parents.map((link, i) => {
//     return(<div>{link}<br/></div>)
//   })
// }

export default ResultCard;
