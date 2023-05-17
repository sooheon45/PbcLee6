
import Vue from 'vue'
import Router from 'vue-router'

Vue.use(Router);


import DeliveryManager from "./components/listers/DeliveryCards"
import DeliveryDetail from "./components/listers/DeliveryDetail"

import OverseasDeliveryManager from "./components/listers/OverseasDeliveryCards"
import OverseasDeliveryDetail from "./components/listers/OverseasDeliveryDetail"


export default new Router({
    // mode: 'history',
    base: process.env.BASE_URL,
    routes: [
            {
                path: '/deliveries',
                name: 'DeliveryManager',
                component: DeliveryManager
            },
            {
                path: '/deliveries/:id',
                name: 'DeliveryDetail',
                component: DeliveryDetail
            },

            {
                path: '/overseasDeliveries',
                name: 'OverseasDeliveryManager',
                component: OverseasDeliveryManager
            },
            {
                path: '/overseasDeliveries/:id',
                name: 'OverseasDeliveryDetail',
                component: OverseasDeliveryDetail
            },



    ]
})
