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
    Summary: ""}
  }
  componentDidMount (props) {
    // extract only the date
    console.log(this.props.data)
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
                  Summary: this.props.data['Summary']});
  }
  render() {
    return (
      <Card className='custom'>
        <CardBody>
          <CardLink className='title' href={this.state.Url}> {this.state.Page_title} </CardLink>
          <small className="text-muted"><span>&#8729;</span> {Math.round(this.state.FinalRank*100)/100}</small>
          <CardSubtitle> {this.state.Url} </CardSubtitle>
          <div className='row'>
          {Object.keys(this.state.Words_mapping).map((word, freq) => {
            return(<Keywords word={word} freq={freq} />)
          })}</div>
        </CardBody>
        <CardBody>
          <CardText>
          {this.state.Summary} <br/>
          <small className="text-muted">
          <b> Parents: </b>
          {
            this.state.Parents.map((link, i) => {
              return(<div>{link}<br/></div>)
            })
          }
          <b> Children: </b>
          {
            this.state.Children.map((link, i) => {
              return(<div>{link}<br/></div>)
            })
          }
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
    );
  }
}

export default ResultCard;
